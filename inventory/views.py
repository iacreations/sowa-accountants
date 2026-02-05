from django.shortcuts import render, redirect, get_object_or_404
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
from .models import Product,BundleItem,Category,Pclass
from sowaf.models import Newsupplier
from accounts.models import Account
from sales.models import InvoiceItem
from .services import InventoryMovement
# Create your views here.
def _dec(v):
    try:
        return Decimal(str(v)) if v not in (None, "",) else Decimal("0.00")
    except Exception:
        return Decimal("0.00")

# working on the product detail
ZERO_DEC = Value(Decimal("0"), output_field=DecimalField(max_digits=18, decimal_places=2))

def inventory_products_list(request):
    """
    Shows products, filtered to Inventory type only.
    """
    qs = (
        Product.objects
        .filter(type="Inventory")
        .select_related("category", "class_field", "supplier")
        .order_by("name")
    )
    return render(request, "products_list.html", {"products": qs})

def product_detail(request, pk: int):
    # Product + common FKs
    product = get_object_or_404(
        Product.objects.select_related(
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
    sold_qty = (
        InvoiceItem.objects.filter(product_id=product.id)
        .aggregate(v=Coalesce(Sum("qty"), ZERO_DEC))
        .get("v") or Decimal("0")
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
            .filter(bundle=product)
        )

    # NEW: Recent Inventory Movements for this product
    movements = (
        InventoryMovement.objects
        .filter(product=product)
        .order_by("-date", "-id")[:30]
    )

    # NEW: Movement totals (in/out) for display
    totals = InventoryMovement.objects.filter(product=product).aggregate(
        total_in=Coalesce(Sum("qty_in"), ZERO_DEC),
        total_out=Coalesce(Sum("qty_out"), ZERO_DEC),
    )
    total_in = totals["total_in"] or ZERO_DEC
    total_out = totals["total_out"] or ZERO_DEC

    # NEW: Stock value (cached qty * cached avg cost)
    avg_cost = Decimal(product.avg_cost or 0)
    stock_value = on_hand * avg_cost

    context = {
        "product": product,
        "sold_qty": sold_qty,
        "on_hand": on_hand,
        "remaining": remaining,
        "out_of_stock": out_of_stock,
        "is_low_stock": is_low_stock,
        "bundle_rows": bundle_rows,

        # NEW context
        "movements": movements,
        "total_in": total_in,
        "total_out": total_out,
        "stock_value": stock_value,
        "avg_cost": avg_cost,
    }
    return render(request, "product_detail.html", context)

def inventory_movements_list(request):
    """
    Full inventory ledger.
    Optional filters:
      ?product_id=1
      ?source_type=BILL
    """
    qs = InventoryMovement.objects.select_related("product").order_by("-date", "-id")

    product_id = request.GET.get("product_id")
    if product_id:
        qs = qs.filter(product_id=product_id)

    source_type = request.GET.get("source_type")
    if source_type:
        qs = qs.filter(source_type=source_type)

    products = Product.objects.filter(type="Inventory").order_by("name")

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

def _find_default_account(name_contains: str):
    """
    Finds an account by name (contains), active only.
    """
    return Account.objects.filter(is_active=True, account_name__icontains=name_contains).order_by("account_name").first()

def _default_inventory_asset_account():
    """
    QuickBooks-like default.
    If you already have an Inventory/Stock account in CoA, it picks it.
    Otherwise, returns None (we still allow saving, but posting will fallback later).
    """
    return (
        _find_default_account("Inventory")
        or _find_default_account("Stock")
        or _find_default_account("Merchandise")
    )

def _default_cogs_account():
    """
    QuickBooks-like default.
    """
    return (
        _find_default_account("Cost of Sales")
        or _find_default_account("Cost of Goods")
        or _find_default_account("COGS")
        or _find_default_account("Expenses")
        or _find_default_account("Expense")
    )


# ======================================================
# ADD PRODUCT
# ======================================================
@transaction.atomic
def add_products(request):
    if request.method == "POST":
        ptype = request.POST.get("type")
        name = (request.POST.get("name") or "").strip()
        sku = (request.POST.get("sku") or "").strip()

        category_id = request.POST.get("category")
        category = Category.objects.filter(pk=category_id).first() if category_id else None

        class_field_id = request.POST.get("class_field")
        class_field = Pclass.objects.filter(pk=class_field_id).first() if class_field_id else None

        sales_description = request.POST.get("sales_description")
        purchase_description = request.POST.get("purchase_description")
        purchase_date = request.POST.get("purchase_date") or None

        sell_checkbox = (request.POST.get("sell_checkbox") == "on")
        purchase_checkbox = (request.POST.get("purchase_checkbox") == "on")
        display_bundle_contents = (request.POST.get("display_bundle_contents") == "on")
        taxable = (request.POST.get("taxable") == "on")

        # ✅ IMPORTANT: Product.quantity is DecimalField -> store Decimal, not int
        quantity = _to_dec(request.POST.get("quantity"), default=Decimal("0.00"))

        sales_price = _to_dec(request.POST.get("sales_price"), default=None)
        purchase_price = _to_dec(request.POST.get("purchase"), default=None)  # your HTML uses name="purchase"

        income_account_id = request.POST.get("income_account")
        income_account = Account.objects.filter(pk=income_account_id).first() if income_account_id else None

        expense_account_id = request.POST.get("expense_account")
        expense_account = Account.objects.filter(pk=expense_account_id).first() if expense_account_id else None

        supplier_id = request.POST.get("supplier")
        supplier = Newsupplier.objects.filter(pk=supplier_id).first() if supplier_id else None

        product = Product.objects.create(
            type=ptype,
            name=name,
            sku=sku,
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
            income_account=income_account,
            expense_account=expense_account,
            purchase_checkbox=purchase_checkbox,
            is_bundle=(ptype == "Bundle"),
            display_bundle_contents=display_bundle_contents,
        )

        # ✅ QuickBooks-style defaults for Inventory items
        # (No form changes required; this just prevents posting crashes.)
        if product.type == "Inventory":
            # only set defaults if empty
            if not getattr(product, "inventory_asset_account_id", None):
                try:
                    product.inventory_asset_account = _default_inventory_asset_account()
                except Exception:
                    pass

            if not getattr(product, "cogs_account_id", None):
                try:
                    product.cogs_account = _default_cogs_account()
                except Exception:
                    pass

            # fallback: if user selected expense_account, let it act as COGS if cogs still empty
            if not getattr(product, "cogs_account_id", None) and product.expense_account_id:
                try:
                    product.cogs_account = product.expense_account
                except Exception:
                    pass

            try:
                product.save(update_fields=["inventory_asset_account", "cogs_account"])
            except Exception:
                # if fields don't exist yet, ignore
                pass

        # bundle items
        if ptype == "Bundle":
            product_ids = request.POST.getlist("bundle_product_id[]")
            quantities = request.POST.getlist("bundle_product_qty[]")
            for prod_id, qty in zip(product_ids, quantities):
                if prod_id:
                    child_product = Product.objects.filter(pk=_to_int(prod_id)).first()
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
            return redirect("sales:sales")
        return redirect("sales:sales")

    context = {
        "products": Product.objects.all(),
        "suppliers": Newsupplier.objects.all(),
        "categories": Category.objects.all(),
        "classes": Pclass.objects.all(),
        "income_accounts": income_accounts_qs(),
        "expense_accounts": expense_accounts_qs(),
        "edit_mode": False,
    }
    return render(request, "Products_and_services_form.html", context)


# ======================================================
# EDIT PRODUCT
# ======================================================
@transaction.atomic
def product_edit(request, pk: int):
    product = get_object_or_404(Product, pk=pk)

    if request.method == "POST":
        ptype = request.POST.get("type") or product.type
        product.type = ptype

        product.name = (request.POST.get("name") or "").strip()
        product.sku  = (request.POST.get("sku") or "").strip()

        category_id     = request.POST.get("category")
        class_field_id  = request.POST.get("class_field")
        supplier_id     = request.POST.get("supplier")
        income_acc_id   = request.POST.get("income_account")
        expense_acc_id  = request.POST.get("expense_account")

        product.category    = Category.objects.filter(pk=category_id).first() if category_id else None
        product.class_field = Pclass.objects.filter(pk=class_field_id).first() if class_field_id else None
        product.supplier    = Newsupplier.objects.filter(pk=supplier_id).first() if supplier_id else None
        product.income_account  = Account.objects.filter(pk=income_acc_id).first() if income_acc_id else None
        product.expense_account = Account.objects.filter(pk=expense_acc_id).first() if expense_acc_id else None

        product.sell_checkbox = (request.POST.get("sell_checkbox") == "on")
        product.purchase_checkbox = (request.POST.get("purchase_checkbox") == "on")
        product.taxable = (request.POST.get("taxable") == "on")
        product.display_bundle_contents = (request.POST.get("display_bundle_contents") == "on")

        product.sales_description = request.POST.get("sales_description") or ""
        product.purchase_description = request.POST.get("purchase_description") or ""
        product.purchase_date = request.POST.get("purchase_date") or None

        product.sales_price = _to_dec(request.POST.get("sales_price"), default=Decimal("0.00"))
        product.purchase_price = _to_dec(request.POST.get("purchase"), default=Decimal("0.00"))  # HTML uses purchase
        product.quantity = _to_dec(request.POST.get("quantity"), default=Decimal("0.00"))       # DecimalField

        product.is_bundle = (ptype == "Bundle")
        product.save()

        # ✅ Set defaults again if Inventory and missing
        if product.type == "Inventory":
            changed = False

            try:
                if not product.inventory_asset_account_id:
                    product.inventory_asset_account = _default_inventory_asset_account()
                    changed = True
            except Exception:
                pass

            try:
                if not product.cogs_account_id:
                    product.cogs_account = _default_cogs_account()
                    changed = True
            except Exception:
                pass

            # fallback: expense_account becomes COGS if still empty
            try:
                if (not product.cogs_account_id) and product.expense_account_id:
                    product.cogs_account = product.expense_account
                    changed = True
            except Exception:
                pass

            if changed:
                try:
                    product.save(update_fields=["inventory_asset_account", "cogs_account"])
                except Exception:
                    pass

        # bundle handling
        if product.is_bundle:
            product.bundleitem_set.all().delete()

            product_ids = request.POST.getlist("bundle_product_id[]")
            quantities  = request.POST.getlist("bundle_product_qty[]")

            for prod_id, qty in zip(product_ids, quantities):
                if prod_id:
                    child = Product.objects.filter(pk=_to_int(prod_id)).first()
                    if child:
                        BundleItem.objects.create(
                            bundle=product,
                            product=child,
                            quantity=_to_int(qty, default=1)
                        )
        else:
            # if you have a different related_name, keep this try/except
            try:
                product.bundle_items.all().delete()
            except Exception:
                pass

        action = request.POST.get("save_action")
        if action == "save&new":
            return redirect("inventory:add-products")
        if action == "save&close":
            return redirect("sales:sales")
        return redirect("inventory:product-detail", pk=product.pk)

    context = {
        "edit_mode": True,
        "product": product,
        "products": Product.objects.all(),
        "suppliers": Newsupplier.objects.all(),
        "categories": Category.objects.all(),
        "classes": Pclass.objects.all(),
        "income_accounts": income_accounts_qs(),
        "expense_accounts": expense_accounts_qs(),
    }
    return render(request, "Products_and_services_form.html", context)

# end
def add_category_ajax(request):
    if request.method == "POST":
        name = request.POST.get("name")
        if not name:
            return JsonResponse({"success": False, "error": "Category name required"})
        
        cat, created = Category.objects.get_or_create(category_type=name)
        return JsonResponse({
            "success": True,
            "id": cat.id,
            "name": cat.category_type,
        })

def add_class_ajax(request):
    if request.method == "POST":
        name = request.POST.get("name")
        if not name:
            return JsonResponse({"success": False, "error": "Class name required"})
        
        cls, created = Pclass.objects.get_or_create(class_name=name)
        return JsonResponse({
            "success": True,
            "id": cls.id,
            "name": cls.class_name,
        })