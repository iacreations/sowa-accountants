from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.http import require_GET, require_POST
from django.core.exceptions import ValidationError
from django.contrib import messages
from django.http import JsonResponse
from django.db import transaction
import json
from openpyxl import Workbook
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from django.utils import timezone
from decimal import Decimal
from django.db.models import Sum, Value, DecimalField
from django.db.models.functions import Coalesce
from django.shortcuts import get_object_or_404, render
import openpyxl
import csv
import io
import os
from django.core.files import File
from django.conf import settings
from django.contrib.auth.decorators import login_required
from accounts.utils import income_accounts_qs, expense_accounts_qs
from .models import Product, BundleItem, Category, Pclass, InventoryLocation, InventoryMovement, StockTransfer, StockTransferLine, Build, BuildLine, BillOfMaterials, BOMItem
from sowaf.models import Newsupplier
from accounts.models import Account
from sales.models import InvoiceItem
from .services import rebuild_movements_for_stock_transfer, get_main_store
from tenancy.permissions import company_required, module_required

# Create your views here.
def _dec(v):
    try:
        return Decimal(str(v)) if v not in (None, "",) else Decimal("0.00")
    except Exception:
        return Decimal("0.00")


# working on the product detail
ZERO_DEC = Value(Decimal("0"), output_field=DecimalField(max_digits=18, decimal_places=2))


@login_required
@company_required
@module_required("inventory")
def inventory_products_list(request):
    """
    Shows all products for the current company.
    """
    company = request.company
    qs = (
        Product.objects.for_company(company)
        .select_related("category", "class_field", "supplier")
        .order_by("name")
    )
    return render(request, "products_list.html", {"products": qs})


@login_required
@company_required
@module_required("inventory")
def product_detail(request, pk: int):
    company = request.company

    # Product + common FKs
    product = get_object_or_404(
        Product.objects.for_company(company).select_related(
            "category",
            "class_field",
            "supplier",
            "income_account",
            "expense_account",
            "inventory_asset_account",
            "cogs_account",
        ),
        pk=pk
    )

    # How many units sold across all invoices
    sold_qty_qs = InvoiceItem.objects.filter(product_id=product.id)
    if hasattr(InvoiceItem, "company_id"):
        sold_qty_qs = sold_qty_qs.filter(company=company)
    elif hasattr(InvoiceItem, "invoice") and hasattr(getattr(InvoiceItem, "invoice", None), "field"):
        pass

    sold_qty = (
        sold_qty_qs.aggregate(v=Coalesce(Sum("qty"), ZERO_DEC)).get("v") or Decimal("0")
    )

    on_hand = Decimal(product.quantity or 0)
    remaining = on_hand - sold_qty
    if remaining < 0:
        remaining = Decimal("0")

    # Status flags
    out_of_stock = remaining <= 0
    low_stock_threshold = Decimal("5")  # tweak if you want
    is_low_stock = (remaining > 0 and remaining <= low_stock_threshold)

    # Bundle rows (if bundle)
    bundle_rows = []
    if getattr(product, "is_bundle", False):
        bundle_rows = list(
            BundleItem.objects
            .select_related("product")
            .filter(bundle=product, product__company=company)
        )

    # NEW: Recent Inventory Movements for this product
    movements = (
        InventoryMovement.objects.for_company(company)
        .filter(product=product)
        .select_related("location")
        .order_by("-date", "-id")[:30]
    )

    # NEW: Movement totals (in/out) for display
    totals = InventoryMovement.objects.for_company(company).filter(product=product).aggregate(
        total_in=Coalesce(Sum("qty_in"), ZERO_DEC),
        total_out=Coalesce(Sum("qty_out"), ZERO_DEC),
    )
    total_in = totals["total_in"] or ZERO_DEC
    total_out = totals["total_out"] or ZERO_DEC

    # FIFO stock value: safe read-only display
    from inventory.fifo import get_available_layers, compute_fifo_cogs
    fifo_layers = get_available_layers(product)
    fifo_unit_cost = Decimal("0")
    stock_value = Decimal("0")
    fifo_warning = None

    try:
        if fifo_layers:
            fifo_unit_cost = Decimal(fifo_layers[0].unit_cost)

        if on_hand > Decimal("0"):
            stock_value = compute_fifo_cogs(product, on_hand)
    except ValueError as e:
        # FIFO stock shortage is not a display error.
        # It only matters when posting a sale.
        fifo_warning = f"⚠️ FIFO Stock Issue: {str(e)}"
        stock_value = Decimal("0")

    # Keep avg_cost for backward compat in template (legacy field)
    avg_cost = Decimal(product.avg_cost or 0)

    context = {
        "product": product,
        "sold_qty": sold_qty,
        "on_hand": on_hand,
        "remaining": remaining,
        "out_of_stock": out_of_stock,
        "is_low_stock": is_low_stock,
        "bundle_rows": bundle_rows,

        # FIFO context
        "movements": movements,
        "total_in": total_in,
        "total_out": total_out,
        "stock_value": stock_value,
        "avg_cost": avg_cost,
        "fifo_unit_cost": fifo_unit_cost,
        "fifo_layers": fifo_layers,
        "fifo_warning": fifo_warning,
    }
    return render(request, "product_detail.html", context)


@login_required
@company_required
@module_required("inventory")
def inventory_movements_list(request):
    """
    Full inventory ledger.
    Optional filters:
      ?product_id=1
      ?source_type=BILL
    """
    company = request.company

    qs = (
        InventoryMovement.objects.for_company(company)
        .select_related("product", "location")
        .order_by("-date", "-id")
    )

    product_id = request.GET.get("product_id")
    if product_id:
        qs = qs.filter(product_id=product_id)

    source_type = request.GET.get("source_type")
    if source_type:
        qs = qs.filter(source_type=source_type)

    products = Product.objects.for_company(company).filter(type="Inventory").order_by("name")

    return render(
        request,
        "movements_list.html",
        {
            "movements": qs[:500],  # keep it safe
            "products": products,
            "selected_product_id": product_id or "",
            "selected_source_type": source_type or "",
        }
    )


# adding a product
# -------------------------
# INTERNAL HELPERS
# -------------------------
def _to_int(val, default=0):
    if val in (None, "", "None", "null"):
        return default
    try:
        return int(val)
    except Exception:
        return default


def _to_dec(val, default=Decimal("0.00")):
    if val in (None, "", "None", "null"):
        return default
    try:
        return Decimal(str(val))
    except Exception:
        return default


def _find_default_account(company, name_contains: str):
    """
    Finds an account by name (contains), active only, company-safe where applicable.
    """
    qs = Account.objects.filter(is_active=True, account_name__icontains=name_contains)
    if hasattr(Account, "company_id"):
        qs = qs.filter(company=company)
    return qs.order_by("account_name").first()


def _default_inventory_asset_account(company):
    """
    QuickBooks-like default: find or create an Inventory Asset account in the COA.
    """
    company_id = getattr(company, "id", company)
    acc = (
        _find_default_account(company, "Inventory Asset")
        or _find_default_account(company, "Stock")
        or _find_default_account(company, "Merchandise")
    )
    if acc:
        return acc

    # Auto-create if not found
    return Account.objects.create(
        company_id=company_id,
        account_name="Inventory Asset",
        account_type="CURRENT_ASSET",
        detail_type="Inventory",
        is_active=True,
        is_subaccount=False,
        parent=None,
        opening_balance=Decimal("0.00"),
        as_of=timezone.localdate(),
    )


def _default_cogs_account(company):
    """
    QuickBooks-like default: find or create a Cost of Goods Sold account in the COA.
    """
    company_id = getattr(company, "id", company)
    acc = (
        _find_default_account(company, "Cost of Sales")
        or _find_default_account(company, "Cost of Goods")
        or _find_default_account(company, "COGS")
    )
    if acc:
        return acc

    # Auto-create if not found
    return Account.objects.create(
        company_id=company_id,
        account_name="Cost of Goods Sold",
        account_type="OPERATING_EXPENSE",
        detail_type="Supplies & Materials - COGS",
        is_active=True,
        is_subaccount=False,
        parent=None,
        opening_balance=Decimal("0.00"),
        as_of=timezone.localdate(),
    )


# ======================================================
# ADD PRODUCT
# ======================================================
@login_required
@company_required
@module_required("inventory")
@transaction.atomic
def add_products(request):
    company = request.company

    if request.method == "POST":
        ptype = request.POST.get("type")
        name = (request.POST.get("name") or "").strip()
        sku = (request.POST.get("sku") or "").strip()

        category_id = request.POST.get("category")
        category = Category.objects.for_company(company).filter(pk=category_id).first() if category_id else None

        class_field_id = request.POST.get("class_field")
        class_field = Pclass.objects.for_company(company).filter(pk=class_field_id).first() if class_field_id else None

        sales_description = request.POST.get("sales_description")
        purchase_description = request.POST.get("purchase_description")
        purchase_date = request.POST.get("purchase_date") or None

        sell_checkbox = (request.POST.get("sell_checkbox") == "on")
        purchase_checkbox = (request.POST.get("purchase_checkbox") == "on")
        display_bundle_contents = (request.POST.get("display_bundle_contents") == "on")
        taxable = (request.POST.get("taxable") == "on")
        track_inventory = (request.POST.get("track_inventory") == "on")

        # IMPORTANT: Product.quantity is DecimalField -> store Decimal, not int
        quantity = _to_dec(request.POST.get("quantity"), default=Decimal("0.00"))

        sales_price = _to_dec(request.POST.get("sales_price"), default=None)
        purchase_price = _to_dec(request.POST.get("purchase"), default=None)  # your HTML uses name="purchase"

        income_account_id = request.POST.get("income_account")
        try:
            income_qs = income_accounts_qs(company=company)
        except TypeError:
            income_qs = income_accounts_qs()
            if hasattr(income_qs, "for_company"):
                income_qs = income_qs.for_company(company)
            elif hasattr(income_qs.model, "company_id"):
                income_qs = income_qs.filter(company=company)
        income_account = income_qs.filter(pk=income_account_id).first() if income_account_id else None

        expense_account_id = request.POST.get("expense_account")
        try:
            expense_qs = expense_accounts_qs(company=company)
        except TypeError:
            expense_qs = expense_accounts_qs()
            if hasattr(expense_qs, "for_company"):
                expense_qs = expense_qs.for_company(company)
            elif hasattr(expense_qs.model, "company_id"):
                expense_qs = expense_qs.filter(company=company)
        expense_account = expense_qs.filter(pk=expense_account_id).first() if expense_account_id else None

        supplier_id = request.POST.get("supplier")
        supplier = Newsupplier.objects.for_company(company).filter(pk=supplier_id).first() if supplier_id else None

        # Check for duplicate SKU before attempting to create
        if sku:
            existing_sku = Product.objects.for_company(company).filter(sku=sku).first()
            if existing_sku:
                messages.error(request, f'A product with SKU "{sku}" already exists in this company.')
                return redirect("inventory:add-products")

        product = Product.objects.create(
            company=company,
            type=ptype,
            name=name,
            sku=sku or None,
            quantity=quantity,
            category=category,
            class_field=class_field,
            sales_description=sales_description,
            purchase_description=purchase_description,
            purchase_date=purchase_date,
            sell_checkbox=sell_checkbox,
            supplier=supplier,
            sales_price=sales_price,
            purchase_price=purchase_price,
            taxable=taxable,
            track_inventory=track_inventory,
            income_account=income_account,
            expense_account=expense_account,
            purchase_checkbox=purchase_checkbox,
            is_bundle=(ptype == "Bundle"),
            display_bundle_contents=display_bundle_contents,
        )

        # When track_inventory is checked, auto-create Inventory Asset + COGS accounts
        if product.track_inventory:

            if not getattr(product, "inventory_asset_account_id", None):
                product.inventory_asset_account = _default_inventory_asset_account(company)

            if not getattr(product, "cogs_account_id", None):
                product.cogs_account = _default_cogs_account(company)

            if not getattr(product, "cogs_account_id", None) and product.expense_account_id:
                product.cogs_account = product.expense_account

            product.save(update_fields=["inventory_asset_account", "cogs_account"])

        # bundle items
        if ptype == "Bundle":
            product_ids = request.POST.getlist("bundle_product_id[]")
            quantities = request.POST.getlist("bundle_product_qty[]")
            for prod_id, qty in zip(product_ids, quantities):
                if prod_id:
                    child_product = Product.objects.for_company(company).filter(pk=_to_int(prod_id)).first()
                    if child_product:
                        BundleItem.objects.create(
                            bundle=product,
                            product=child_product,
                            quantity=_to_int(qty, default=1)
                        )

        action = request.POST.get("save_action")
        if action == "save&new":
            return redirect("inventory:add-products")
        elif action == "save&close":
            return redirect("inventory:products-list")
        return redirect("inventory:products-list")

    try:
        income_qs = income_accounts_qs(company=company)
    except TypeError:
        income_qs = income_accounts_qs()
        if hasattr(income_qs, "for_company"):
            income_qs = income_qs.for_company(company)
        elif hasattr(income_qs.model, "company_id"):
            income_qs = income_qs.filter(company=company)

    try:
        expense_qs = expense_accounts_qs(company=company)
    except TypeError:
        expense_qs = expense_accounts_qs()
        if hasattr(expense_qs, "for_company"):
            expense_qs = expense_qs.for_company(company)
        elif hasattr(expense_qs.model, "company_id"):
            expense_qs = expense_qs.filter(company=company)

    context = {
        "products": Product.objects.for_company(company).all(),
        "suppliers": Newsupplier.objects.for_company(company).all(),
        "categories": Category.objects.for_company(company).all(),
        "classes": Pclass.objects.for_company(company).all(),
        "income_accounts": income_qs,
        "expense_accounts": expense_qs,
        "edit_mode": False,
    }
    return render(request, "Products_and_services_form.html", context)


# ======================================================
# EDIT PRODUCT
# ======================================================
@login_required
@company_required
@module_required("inventory")
@transaction.atomic
def product_edit(request, pk: int):
    company = request.company
    product = get_object_or_404(Product.objects.for_company(company), pk=pk)

    if request.method == "POST":
        ptype = request.POST.get("type") or product.type
        product.type = ptype

        product.name = (request.POST.get("name") or "").strip()
        product.sku = (request.POST.get("sku") or "").strip()

        category_id = request.POST.get("category")
        class_field_id = request.POST.get("class_field")
        supplier_id = request.POST.get("supplier")
        income_acc_id = request.POST.get("income_account")
        expense_acc_id = request.POST.get("expense_account")

        product.category = Category.objects.for_company(company).filter(pk=category_id).first() if category_id else None
        product.class_field = Pclass.objects.for_company(company).filter(pk=class_field_id).first() if class_field_id else None
        product.supplier = Newsupplier.objects.for_company(company).filter(pk=supplier_id).first() if supplier_id else None

        try:
            income_qs = income_accounts_qs(company=company)
        except TypeError:
            income_qs = income_accounts_qs()
            if hasattr(income_qs, "for_company"):
                income_qs = income_qs.for_company(company)
            elif hasattr(income_qs.model, "company_id"):
                income_qs = income_qs.filter(company=company)
        product.income_account = income_qs.filter(pk=income_acc_id).first() if income_acc_id else None

        try:
            expense_qs = expense_accounts_qs(company=company)
        except TypeError:
            expense_qs = expense_accounts_qs()
            if hasattr(expense_qs, "for_company"):
                expense_qs = expense_qs.for_company(company)
            elif hasattr(expense_qs.model, "company_id"):
                expense_qs = expense_qs.filter(company=company)
        product.expense_account = expense_qs.filter(pk=expense_acc_id).first() if expense_acc_id else None

        product.sell_checkbox = (request.POST.get("sell_checkbox") == "on")
        product.purchase_checkbox = (request.POST.get("purchase_checkbox") == "on")
        product.taxable = (request.POST.get("taxable") == "on")
        product.track_inventory = (request.POST.get("track_inventory") == "on")
        product.display_bundle_contents = (request.POST.get("display_bundle_contents") == "on")

        product.sales_description = request.POST.get("sales_description") or ""
        product.purchase_description = request.POST.get("purchase_description") or ""
        product.purchase_date = request.POST.get("purchase_date") or None

        product.sales_price = _to_dec(request.POST.get("sales_price"), default=Decimal("0.00"))
        product.purchase_price = _to_dec(request.POST.get("purchase"), default=Decimal("0.00"))  # HTML uses purchase
        product.quantity = _to_dec(request.POST.get("quantity"), default=Decimal("0.00"))       # DecimalField

        product.is_bundle = (ptype == "Bundle")
        try:
            product.save()
        except ValidationError as e:
            flat = "; ".join(
                msg for msgs in e.message_dict.values() for msg in msgs
            ) if hasattr(e, "message_dict") else str(e)
            messages.error(request, f"Could not save product: {flat}")
            return redirect(request.path)
        except Exception as e:
            if "uniq_product_sku_per_company" in str(e):
                messages.error(request, "A product with this SKU already exists for this company.")
                return redirect(request.path)
            raise

        # When track_inventory is checked, auto-create Inventory Asset + COGS accounts
        if product.track_inventory:
            changed = False

            if not product.inventory_asset_account_id:
                product.inventory_asset_account = _default_inventory_asset_account(company)
                changed = True

            if not product.cogs_account_id:
                product.cogs_account = _default_cogs_account(company)
                changed = True

            if not product.cogs_account_id and product.expense_account_id:
                product.cogs_account = product.expense_account
                changed = True

            if changed:
                Product.objects.filter(pk=product.pk).update(
                    inventory_asset_account=product.inventory_asset_account,
                    cogs_account=product.cogs_account,
                )

        # bundle handling
        if product.is_bundle:
            try:
                product.bundleitem_set.all().delete()
            except Exception:
                try:
                    product.bundle_items.all().delete()
                except Exception:
                    pass

            product_ids = request.POST.getlist("bundle_product_id[]")
            quantities = request.POST.getlist("bundle_product_qty[]")

            for prod_id, qty in zip(product_ids, quantities):
                if prod_id:
                    child = Product.objects.for_company(company).filter(pk=_to_int(prod_id)).first()
                    if child:
                        BundleItem.objects.create(
                            bundle=product,
                            product=child,
                            quantity=_to_int(qty, default=1)
                        )
        else:
            try:
                product.bundle_items.all().delete()
            except Exception:
                try:
                    product.bundleitem_set.all().delete()
                except Exception:
                    pass

        action = request.POST.get("save_action")
        if action == "save&new":
            return redirect("inventory:add-products")
        if action == "save&close":
            return redirect("inventory:products-list")
        return redirect("inventory:product-detail", pk=product.pk)

    try:
        income_qs = income_accounts_qs(company=company)
    except TypeError:
        income_qs = income_accounts_qs()
        if hasattr(income_qs, "for_company"):
            income_qs = income_qs.for_company(company)
        elif hasattr(income_qs.model, "company_id"):
            income_qs = income_qs.filter(company=company)

    try:
        expense_qs = expense_accounts_qs(company=company)
    except TypeError:
        expense_qs = expense_accounts_qs()
        if hasattr(expense_qs, "for_company"):
            expense_qs = expense_qs.for_company(company)
        elif hasattr(expense_qs.model, "company_id"):
            expense_qs = expense_qs.filter(company=company)

    context = {
        "edit_mode": True,
        "product": product,
        "products": Product.objects.for_company(company).all(),
        "suppliers": Newsupplier.objects.for_company(company).all(),
        "categories": Category.objects.for_company(company).all(),
        "classes": Pclass.objects.for_company(company).all(),
        "income_accounts": income_qs,
        "expense_accounts": expense_qs,
    }
    return render(request, "Products_and_services_form.html", context)


# end
@login_required
@company_required
@module_required("inventory")
def add_category_ajax(request):
    company = request.company
    if request.method == "POST":
        name = request.POST.get("name")
        if not name:
            return JsonResponse({"success": False, "error": "Category name required"})

        cat, created = Category.objects.get_or_create(
            company=company, category_type=name,
        )
        return JsonResponse({
            "success": True,
            "id": cat.id,
            "name": cat.category_type,
        })


@login_required
@company_required
@module_required("inventory")
def add_class_ajax(request):
    company = request.company
    if request.method == "POST":
        name = request.POST.get("name")
        if not name:
            return JsonResponse({"success": False, "error": "Class name required"})

        cls, created = Pclass.objects.get_or_create(
            company=company, class_name=name,
        )
        return JsonResponse({
            "success": True,
            "id": cls.id,
            "name": cls.class_name,
        })


@login_required
@company_required
@module_required("inventory")
@require_GET
def suppliers_list_json(request):
    company = request.company
    suppliers = (
        Newsupplier.objects
        .for_company(company)
        .filter(is_active=True)
        .order_by("company_name", "id")
    )
    return JsonResponse({
        "ok": True,
        "suppliers": [
            {
                "id": supplier.id,
                "name": supplier.company_name or f"Supplier {supplier.id}",
            }
            for supplier in suppliers
        ],
    })


def _dec(val, default="0"):
    try:
        s = str(val).strip()
        if s == "":
            s = str(default)
        return Decimal(s)
    except Exception:
        return Decimal(str(default))


# stock transfer
@login_required
@company_required
@module_required("inventory")
@transaction.atomic
def add_stock_transfer(request):
    company = request.company

    if request.method == "POST":
        from_loc_id = (request.POST.get("from_location") or "").strip()
        to_loc_id = (request.POST.get("to_location") or "").strip()
        transfer_date = request.POST.get("transfer_date") or None
        memo = request.POST.get("memo") or ""

        if not from_loc_id or not to_loc_id:
            return redirect("inventory:add-stock-transfer")

        if from_loc_id == to_loc_id:
            return redirect("inventory:add-stock-transfer")

        from_loc = get_object_or_404(InventoryLocation.objects.for_company(company), pk=from_loc_id)
        to_loc = get_object_or_404(InventoryLocation.objects.for_company(company), pk=to_loc_id)

        # parse date
        if transfer_date:
            try:
                tdate = timezone.datetime.strptime(transfer_date, "%Y-%m-%d").date()
            except Exception:
                tdate = timezone.localdate()
        else:
            tdate = timezone.localdate()

        transfer = StockTransfer.objects.create(
            company=company,
            from_location=from_loc,
            to_location=to_loc,
            transfer_date=tdate,
            memo=memo,
        )

        product_ids = request.POST.getlist("product[]")
        qtys = request.POST.getlist("qty[]")

        lines = []
        for i, pid in enumerate(product_ids):
            if not pid:
                continue
            product = Product.objects.for_company(company).filter(pk=pid).first()
            if not product:
                continue

            qty = _dec(qtys[i] if i < len(qtys) else "0", "0")
            if qty <= 0:
                continue

            lines.append(StockTransferLine(
                transfer=transfer,
                product=product,
                qty=qty,
            ))

        if not lines:
            transfer.delete()
            return redirect("inventory:add-stock-transfer")

        StockTransferLine.objects.bulk_create(lines)

        # Build movements (OUT from A, IN to B) — no GL posting
        try:
            rebuild_movements_for_stock_transfer(transfer)
        except ValueError as e:
            transfer.delete()
            messages.error(request, str(e))
            return redirect("inventory:add-stock-transfer")

        return redirect("inventory:stock-transfer-list")

    context = {
        "locations": InventoryLocation.objects.for_company(company).filter(is_active=True).order_by("name"),
        "products": Product.objects.for_company(company).all().order_by("name"),
        "today": timezone.localdate(),
    }
    return render(request, "stock_transfer_form.html", context)


@login_required
@company_required
@module_required("inventory")
def stock_transfer_list(request):
    company = request.company
    transfers = StockTransfer.objects.for_company(company).select_related("from_location", "to_location").all()
    return render(request, "stock_transfer_list.html", {"transfers": transfers})


@login_required
@company_required
@module_required("inventory")
def stock_transfer_detail(request, pk: int):
    company = request.company
    transfer = get_object_or_404(
        StockTransfer.objects.for_company(company).select_related("from_location", "to_location"),
        pk=pk
    )
    lines = transfer.lines.select_related("product").all()
    return render(request, "stock_transfer_detail.html", {"transfer": transfer, "lines": lines})


def _get_default_location(company):
    loc = InventoryLocation.objects.for_company(company).filter(is_default=True, is_active=True).first()
    if not loc:
        store = get_main_store(company) if callable(get_main_store) else None
        create_kwargs = {
            "company": company,
            "name": "Main Store",
            "is_default": True,
            "is_active": True,
        }
        if store is not None:
            create_kwargs["store"] = store
        loc = InventoryLocation.objects.create(**create_kwargs)

    InventoryLocation.objects.for_company(company).exclude(id=loc.id).update(is_default=False)
    return loc


@login_required
@company_required
@module_required("inventory")
@require_GET
def locations_list_json(request):
    company = request.company
    locs = InventoryLocation.objects.for_company(company).filter(is_active=True).order_by("-is_default", "name")
    return JsonResponse({
        "ok": True,
        "locations": [{"id": l.id, "name": l.name, "is_default": l.is_default} for l in locs]
    })


@login_required
@company_required
@module_required("inventory")
@require_POST
def location_create_json(request):
    company = request.company
    name = (request.POST.get("name") or "").strip()
    if not name:
        return JsonResponse({"ok": False, "error": "Location name is required."}, status=400)

    loc = InventoryLocation.objects.for_company(company).filter(name__iexact=name).first()
    if loc:
        if not loc.is_active:
            loc.is_active = True
            loc.save(update_fields=["is_active"])
        return JsonResponse({"ok": True, "location": {"id": loc.id, "name": loc.name}})

    store = get_main_store(company) if callable(get_main_store) else None
    create_kwargs = {
        "company": company,
        "name": name,
        "is_default": False,
        "is_active": True,
    }
    if store is not None:
        create_kwargs["store"] = store

    loc = InventoryLocation.objects.create(**create_kwargs)
    _get_default_location(company)  # ensures default exists
    return JsonResponse({"ok": True, "location": {"id": loc.id, "name": loc.name}})


@login_required
@company_required
@module_required("inventory")
@require_POST
@transaction.atomic
def add_location_ajax(request):
    company = request.company
    name = (request.POST.get("name") or "").strip()
    if not name:
        return JsonResponse({"success": False, "error": "Location name is required."})

    store = get_main_store(company) if callable(get_main_store) else None

    loc, created = InventoryLocation.objects.for_company(company).get_or_create(
        store=store,
        name=name,
        defaults={"company": company, "is_active": True, "is_default": False},
    )

    if not loc.is_active:
        loc.is_active = True
        loc.save(update_fields=["is_active"])

    return JsonResponse({"success": True, "id": loc.id, "name": loc.name})


# ==========================================================
# ASSEMBLY BUILDS
# ==========================================================

@login_required
@company_required
@module_required("inventory")
def build_list(request):
    company = request.company
    builds = (
        Build.objects.for_company(company)
        .select_related("finished_product")
        .order_by("-build_date", "-id")
    )
    return render(request, "build_list.html", {"builds": builds})


@login_required
@company_required
@module_required("inventory")
def build_detail(request, pk: int):
    company = request.company
    build = get_object_or_404(Build.objects.for_company(company).select_related("finished_product"), pk=pk)
    lines = build.lines.select_related("component").all()
    return render(request, "build_detail.html", {"build": build, "lines": lines})


@login_required
@company_required
@module_required("inventory")
@transaction.atomic
def add_build(request):
    company = request.company

    if request.method == "POST":
        finished_id = request.POST.get("finished_product")
        build_qty = _dec(request.POST.get("build_qty", "1"), "1")
        build_date = request.POST.get("build_date") or timezone.localdate()
        memo = request.POST.get("memo", "")
        location_id = request.POST.get("location") or None
        bom_id = request.POST.get("bom") or None
        action = request.POST.get("action", "draft")  # "draft" or "complete"

        finished = Product.objects.for_company(company).filter(pk=finished_id).first()
        if not finished:
            return redirect("inventory:add-build")

        location = None
        if location_id:
            location = InventoryLocation.objects.for_company(company).filter(pk=location_id).first()

        bom = None
        if bom_id:
            bom = BillOfMaterials.objects.for_company(company).filter(pk=bom_id).first()

        build = Build.objects.create(
            company=company,
            finished_product=finished,
            build_qty=build_qty,
            build_date=build_date,
            location=location,
            bom=bom,
            memo=memo,
            status="DRAFT",
            created_by=request.user if hasattr(request.user, "id") else None,
        )

        # If BOM selected, auto-load components from it
        if bom:
            from inventory.assembly_engine import load_bom_into_build
            load_bom_into_build(build, bom)
        else:
            # Parse manually entered component lines
            comp_ids = request.POST.getlist("component_id[]")
            comp_qtys = request.POST.getlist("qty_per_unit[]")

            bulk = []
            for i, cid in enumerate(comp_ids):
                if not cid:
                    continue
                component = Product.objects.for_company(company).filter(pk=cid).first()
                if not component:
                    continue
                qty = _dec(comp_qtys[i] if i < len(comp_qtys) else "0", "0")
                if qty <= 0:
                    continue
                bulk.append(BuildLine(build=build, component=component, qty_per_unit=qty))

            if bulk:
                BuildLine.objects.bulk_create(bulk)

        if not build.lines.exists():
            build.delete()
            messages.error(request, "Assembly must have at least one component line.")
            return redirect("inventory:add-build")

        # Optionally complete immediately
        if action == "complete":
            try:
                from inventory.assembly_engine import complete_assembly
                complete_assembly(build, completed_by=request.user if hasattr(request.user, "id") else None)
                messages.success(request, f"Assembly {build.assembly_number} completed successfully.")
            except (ValueError, Exception) as e:
                messages.error(request, f"Could not complete assembly: {e}")

        return redirect("inventory:build-detail", pk=build.pk)

    products = Product.objects.for_company(company).all().order_by("name")
    boms = BillOfMaterials.objects.for_company(company).filter(is_active=True).select_related("finished_product").order_by("finished_product__name")
    locations = InventoryLocation.objects.for_company(company).filter(is_active=True).order_by("name")
    return render(request, "build_form.html", {
        "products": products,
        "boms": boms,
        "locations": locations,
        "today": timezone.localdate(),
    })


@login_required
@company_required
@module_required("inventory")
@transaction.atomic
def complete_build_view(request, pk: int):
    from inventory.assembly_engine import complete_assembly

    company = request.company
    build = get_object_or_404(Build.objects.for_company(company), pk=pk)

    if build.status == "COMPLETED":
        return redirect("inventory:build-detail", pk=build.pk)

    try:
        complete_assembly(build, completed_by=request.user if hasattr(request.user, "id") else None)
        messages.success(request, f"Assembly {build.assembly_number} completed successfully.")
    except ValueError as e:
        messages.error(request, str(e))

    return redirect("inventory:build-detail", pk=build.pk)


@login_required
@company_required
@module_required("inventory")
@require_POST
@transaction.atomic
def cancel_build_view(request, pk: int):
    from inventory.assembly_engine import cancel_assembly

    company = request.company
    build = get_object_or_404(Build.objects.for_company(company), pk=pk)

    try:
        cancel_assembly(build)
        messages.success(request, f"Assembly {build.assembly_number} cancelled.")
    except ValueError as e:
        messages.error(request, str(e))

    return redirect("inventory:build-detail", pk=build.pk)


@login_required
@company_required
@module_required("inventory")
@require_POST
@transaction.atomic
def reverse_build_view(request, pk: int):
    from inventory.assembly_engine import reverse_assembly

    company = request.company
    build = get_object_or_404(Build.objects.for_company(company), pk=pk)

    try:
        reverse_assembly(build)
        messages.success(request, f"Assembly {build.assembly_number} reversed successfully.")
    except ValueError as e:
        messages.error(request, str(e))

    return redirect("inventory:build-detail", pk=build.pk)

# ==========================================================
# PHASE 7: Reports and StockAdjustment views
# ==========================================================

@login_required
@company_required
@module_required("inventory")
def report_movement_ledger(request):
    """Inventory Movement Ledger with filters."""
    company = request.company

    movements = InventoryMovement.objects.for_company(company).select_related(
        "product", "location"
    ).order_by("date", "id")

    product_id = request.GET.get("product")
    source_type = request.GET.get("source_type")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    location_id = request.GET.get("location")

    if product_id:
        movements = movements.filter(product_id=product_id)
    if source_type:
        movements = movements.filter(source_type=source_type)
    if date_from:
        movements = movements.filter(date__gte=date_from)
    if date_to:
        movements = movements.filter(date__lte=date_to)
    if location_id:
        movements = movements.filter(location_id=location_id)

    products = Product.objects.for_company(company).order_by("name")
    locations = InventoryLocation.objects.for_company(company).order_by("name")
    source_types = InventoryMovement.SOURCE_TYPES

    running_movements = []
    balance = Decimal("0.00")
    current_product_id = None

    for mv in movements:
        if mv.product_id != current_product_id:
            balance = Decimal("0.00")
            current_product_id = mv.product_id
        balance += (mv.qty_in or Decimal("0.00")) - (mv.qty_out or Decimal("0.00"))
        running_movements.append({"movement": mv, "balance": balance})

    return render(request, "report_movement_ledger.html", {
        "movements": running_movements,
        "products": products,
        "locations": locations,
        "source_types": source_types,
        "filters": {
            "product": product_id, "source_type": source_type,
            "date_from": date_from, "date_to": date_to, "location": location_id,
        },
    })


@login_required
@company_required
@module_required("inventory")
def report_stock_valuation(request):
    """Stock Valuation Report - FIFO value per product."""
    from .fifo import get_available_layers

    company = request.company
    products = Product.objects.for_company(company).filter(type="Inventory").order_by("name")

    valuation_data = []
    total_value = Decimal("0.00")

    for p in products:
        layers = get_available_layers(p)
        qty_on_hand = sum(Decimal(str(l.qty_remaining)) for l in layers)
        fifo_value = sum(Decimal(str(l.qty_remaining)) * Decimal(str(l.unit_cost)) for l in layers)
        total_value += fifo_value
        valuation_data.append({
            "product": p,
            "qty_on_hand": qty_on_hand,
            "layers": layers,
            "fifo_value": fifo_value,
        })

    return render(request, "report_stock_valuation.html", {
        "valuation_data": valuation_data,
        "total_value": total_value,
    })


@login_required
@company_required
@module_required("inventory")
def report_stock_aging(request):
    """Stock Aging Report - age bands and slow-moving products."""
    from .fifo import get_available_layers

    company = request.company
    today = timezone.localdate()

    products = Product.objects.for_company(company).filter(type="Inventory").order_by("name")

    aging_data = []
    for p in products:
        layers = get_available_layers(p)
        if not layers:
            continue
        oldest_layer = layers[0]
        if oldest_layer.date_created:
            age_days = (today - oldest_layer.date_created).days
        else:
            age_days = 0

        if age_days < 30:
            band = "0-30 days"
        elif age_days < 90:
            band = "30-90 days"
        elif age_days < 180:
            band = "90-180 days"
        else:
            band = "180+ days (Slow/Dead)"

        aging_data.append({
            "product": p,
            "oldest_date": oldest_layer.date_created,
            "age_days": age_days,
            "band": band,
            "qty_on_hand": sum(Decimal(str(l.qty_remaining)) for l in layers),
        })

    return render(request, "report_stock_aging.html", {"aging_data": aging_data})


@login_required
@company_required
@module_required("inventory")
def report_reorder(request):
    """Reorder Report - products below reorder level."""
    from .fifo import get_available_layers

    company = request.company
    show_all = request.GET.get("show_all") == "1"

    products = Product.objects.for_company(company).filter(type="Inventory").select_related("supplier").order_by("name")

    reorder_data = []
    for p in products:
        layers = get_available_layers(p)
        qty_on_hand = sum(Decimal(str(l.qty_remaining)) for l in layers)

        try:
            threshold = p.alert_threshold.low_stock_threshold
        except Exception:
            threshold = Decimal("10.00")

        needs_reorder = qty_on_hand <= threshold

        if show_all or needs_reorder:
            reorder_data.append({
                "product": p,
                "qty_on_hand": qty_on_hand,
                "reorder_level": threshold,
                "needs_reorder": needs_reorder,
                "supplier": p.supplier,
            })

    return render(request, "report_reorder.html", {"reorder_data": reorder_data, "show_all": show_all})


@login_required
@company_required
@module_required("inventory")
def report_expiry(request):
    """Expiry Report - batches expiring soon."""
    from .models import Batch

    company = request.company
    today = timezone.localdate()
    days_ahead = int(request.GET.get("days", 90))

    batches = Batch.objects.for_company(company).select_related("product", "location").filter(
        status="active",
        expiry_date__isnull=False,
    ).order_by("expiry_date")

    expiry_data = []
    for b in batches:
        days_to_expiry = (b.expiry_date - today).days if b.expiry_date else None
        if days_to_expiry is not None and days_to_expiry <= days_ahead:
            severity = "critical" if days_to_expiry < 30 else "warning"
            expiry_data.append({"batch": b, "days_to_expiry": days_to_expiry, "severity": severity})

    return render(request, "report_expiry.html", {"expiry_data": expiry_data, "days_ahead": days_ahead})


@login_required
@company_required
@module_required("inventory")
def stock_adjustment_list(request):
    from .models import StockAdjustment
    company = request.company
    adjustments = StockAdjustment.objects.for_company(company).order_by("-date", "-id")
    return render(request, "stock_adjustment_list.html", {"adjustments": adjustments})


@login_required
@company_required
@module_required("inventory")
def stock_adjustment_create(request):
    from .models import StockAdjustment, StockAdjustmentLine
    company = request.company
    products = Product.objects.for_company(company).filter(type="Inventory").order_by("name")

    if request.method == "POST":
        try:
            with transaction.atomic():
                adj = StockAdjustment(
                    company=company,
                    date=request.POST.get("date") or timezone.localdate(),
                    reason=request.POST.get("reason", "other"),
                    memo=request.POST.get("memo", ""),
                    status="draft",
                )
                adj.save()

                product_ids = request.POST.getlist("product_id[]")
                qty_increases = request.POST.getlist("qty_increase[]")
                qty_decreases = request.POST.getlist("qty_decrease[]")
                unit_costs = request.POST.getlist("unit_cost[]")

                for i, pid in enumerate(product_ids):
                    if not pid:
                        continue
                    try:
                        p = Product.objects.for_company(company).get(id=pid)
                    except Product.DoesNotExist:
                        continue

                    line = StockAdjustmentLine(
                        adjustment=adj,
                        product=p,
                        qty_increase=_dec(qty_increases[i] if i < len(qty_increases) else 0),
                        qty_decrease=_dec(qty_decreases[i] if i < len(qty_decreases) else 0),
                        unit_cost=_dec(unit_costs[i] if i < len(unit_costs) else 0),
                    )
                    line.save()

                messages.success(request, f"Stock adjustment #{adj.id} created.")
                return redirect("inventory:stock-adjustment-detail", pk=adj.id)
        except Exception as e:
            messages.error(request, f"Error creating adjustment: {e}")

    return render(request, "stock_adjustment_form.html", {
        "products": products,
        "reason_choices": StockAdjustment.REASON_CHOICES,
    })


@login_required
@company_required
@module_required("inventory")
def stock_adjustment_detail(request, pk):
    from .models import StockAdjustment
    company = request.company
    adjustment = get_object_or_404(
        StockAdjustment.objects.for_company(company).prefetch_related("lines__product"), pk=pk
    )
    return render(request, "stock_adjustment_detail.html", {"adjustment": adjustment})


@login_required
@company_required
@module_required("inventory")
def stock_adjustment_post(request, pk):
    from .models import StockAdjustment
    from .services import rebuild_movements_for_stock_adjustment
    company = request.company
    adjustment = get_object_or_404(StockAdjustment.objects.for_company(company), pk=pk)

    if request.method == "POST":
        if adjustment.status != "draft":
            messages.error(request, "Only draft adjustments can be posted.")
            return redirect("inventory:stock-adjustment-detail", pk=pk)
        try:
            with transaction.atomic():
                adjustment.status = "posted"
                adjustment._skip_inventory_signal = True
                adjustment.save(update_fields=["status"])
                rebuild_movements_for_stock_adjustment(adjustment)
            messages.success(request, f"Adjustment #{pk} posted successfully.")
        except Exception as e:
            messages.error(request, f"Could not post adjustment: {e}")

    return redirect("inventory:stock-adjustment-detail", pk=pk)


@login_required
@company_required
@module_required("inventory")
def inventory_alerts(request):
    from .models import InventoryAlert
    company = request.company
    alert_type = request.GET.get("type")
    resolved = request.GET.get("resolved") == "1"

    alerts = InventoryAlert.objects.for_company(company).select_related("product").order_by("-created_at")

    if alert_type:
        alerts = alerts.filter(alert_type=alert_type)
    if resolved:
        alerts = alerts.filter(resolved_at__isnull=False)
    else:
        alerts = alerts.filter(resolved_at__isnull=True)

    return render(request, "inventory_alerts.html", {
        "alerts": alerts,
        "alert_types": InventoryAlert.ALERT_TYPES,
    })


# ==========================================================
# BILL OF MATERIALS (BOM)
# ==========================================================

@login_required
@company_required
@module_required("inventory")
def bom_list(request):
    company = request.company
    boms = (
        BillOfMaterials.objects.for_company(company)
        .select_related("finished_product")
        .prefetch_related("items__component_item")
        .order_by("finished_product__name", "-version")
    )
    return render(request, "bom_list.html", {"boms": boms})


@login_required
@company_required
@module_required("inventory")
def bom_detail(request, pk):
    company = request.company
    bom = get_object_or_404(
        BillOfMaterials.objects.for_company(company).select_related("finished_product"),
        pk=pk,
    )
    items = bom.items.select_related("component_item").all()
    return render(request, "bom_detail.html", {"bom": bom, "items": items})


@login_required
@company_required
@module_required("inventory")
@transaction.atomic
def bom_create(request):
    company = request.company

    if request.method == "POST":
        finished_id = request.POST.get("finished_product")
        notes = request.POST.get("notes", "")

        finished = Product.objects.for_company(company).filter(pk=finished_id).first()
        if not finished:
            messages.error(request, "Please select a valid finished product.")
            return redirect("inventory:bom-create")

        # Determine next version for this product
        latest = (
            BillOfMaterials.objects.for_company(company)
            .filter(finished_product=finished)
            .order_by("-version")
            .first()
        )
        version = (latest.version + 1) if latest else 1

        bom = BillOfMaterials.objects.create(
            company=company,
            finished_product=finished,
            version=version,
            is_active=True,
            notes=notes,
        )

        comp_ids = request.POST.getlist("component_id[]")
        comp_qtys = request.POST.getlist("quantity_required[]")
        comp_costs = request.POST.getlist("unit_cost[]")

        bulk = []
        for i, cid in enumerate(comp_ids):
            if not cid:
                continue
            comp = Product.objects.for_company(company).filter(pk=cid).first()
            if not comp:
                continue
            qty = _dec(comp_qtys[i] if i < len(comp_qtys) else "0", "0")
            if qty <= 0:
                continue
            cost_val = _dec(comp_costs[i] if i < len(comp_costs) else "0", None)
            bulk.append(BOMItem(
                bom=bom,
                component_item=comp,
                quantity_required=qty,
                unit_cost=cost_val if cost_val and cost_val > 0 else None,
            ))

        if not bulk:
            bom.delete()
            messages.error(request, "BOM must have at least one component.")
            return redirect("inventory:bom-create")

        BOMItem.objects.bulk_create(bulk)
        messages.success(request, f"BOM v{version} created for {finished.name}.")
        return redirect("inventory:bom-detail", pk=bom.pk)

    products = Product.objects.for_company(company).order_by("name")
    return render(request, "bom_form.html", {"products": products})


@login_required
@company_required
@module_required("inventory")
@require_POST
@transaction.atomic
def bom_delete(request, pk):
    company = request.company
    bom = get_object_or_404(BillOfMaterials.objects.for_company(company), pk=pk)

    if bom.builds.filter(status="COMPLETED").exists():
        messages.error(request, "Cannot delete a BOM that has completed assemblies.")
        return redirect("inventory:bom-detail", pk=pk)

    bom.delete()
    messages.success(request, "BOM deleted.")
    return redirect("inventory:bom-list")


# ==========================================================
# ASSEMBLY REPORTS
# ==========================================================

@login_required
@company_required
@module_required("inventory")
def report_assembly(request):
    """Assembly Summary Report."""
    from inventory.assembly_engine import assembly_report_data

    company = request.company
    date_from = request.GET.get("date_from") or None
    date_to = request.GET.get("date_to") or None
    status = request.GET.get("status") or None

    builds = assembly_report_data(company, date_from=date_from, date_to=date_to, status=status)

    return render(request, "report_assembly.html", {
        "builds": builds,
        "status_choices": Build.STATUS_CHOICES,
        "filters": {"date_from": date_from, "date_to": date_to, "status": status},
    })


@login_required
@company_required
@module_required("inventory")
def report_component_consumption(request):
    """Component Consumption Report."""
    from inventory.assembly_engine import component_consumption_report

    company = request.company
    date_from = request.GET.get("date_from") or None
    date_to = request.GET.get("date_to") or None

    data = component_consumption_report(company, date_from=date_from, date_to=date_to)

    return render(request, "report_component_consumption.html", {
        "data": data,
        "filters": {"date_from": date_from, "date_to": date_to},
    })


@login_required
@company_required
@module_required("inventory")
def report_wip(request):
    """WIP (Work In Progress) Report."""
    from inventory.assembly_engine import wip_report_data

    company = request.company
    builds = wip_report_data(company)

    return render(request, "report_wip.html", {"builds": builds})


# ==========================================================
# ASSEMBLY IMPORT / EXPORT
# ==========================================================

@login_required
@company_required
@module_required("inventory")
def assembly_export_csv(request):
    """Export assemblies to CSV download."""
    from inventory.assembly_engine import export_assemblies_csv
    from django.http import HttpResponse

    company = request.company
    date_from = request.GET.get("date_from") or None
    date_to = request.GET.get("date_to") or None

    csv_content = export_assemblies_csv(company, date_from=date_from, date_to=date_to)

    response = HttpResponse(csv_content, content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="assemblies_export.csv"'
    return response


@login_required
@company_required
@module_required("inventory")
def assembly_import_csv(request):
    """Import assemblies from CSV upload."""
    from inventory.assembly_engine import import_assemblies_csv

    company = request.company
    result = None

    if request.method == "POST":
        csv_file = request.FILES.get("csv_file")
        if not csv_file:
            messages.error(request, "Please upload a CSV file.")
        else:
            try:
                csv_content = csv_file.read().decode("utf-8")
                result = import_assemblies_csv(
                    csv_content, company,
                    created_by=request.user if hasattr(request.user, "id") else None,
                )
                if result["created"]:
                    messages.success(request, f"Imported {result['created']} assemblies.")
                for err in result["errors"]:
                    messages.warning(request, err)
            except Exception as e:
                messages.error(request, f"Import failed: {e}")

    return render(request, "assembly_import.html", {"result": result})

