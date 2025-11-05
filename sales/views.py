from django.shortcuts import render, redirect, get_object_or_404
from django.http import HttpResponse
from openpyxl import Workbook
from tempfile import NamedTemporaryFile
from datetime import date, timedelta, datetime
from django.utils import timezone
from decimal import Decimal
from django.urls import reverse
from django.db import transaction
from django.templatetags.static import static
from django.db.models import DecimalField, Q
import openpyxl
import csv
import io
import os
from django.db.models.functions import Coalesce, Cast
from django.core.files import File
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from .models import Newinvoice,InvoiceItem,Product,Payment,PaymentInvoice,SalesReceipt,SalesReceiptLine
from sowaf.models import Newcustomer
from django.http import JsonResponse
from django.db.models import Sum, F, Value
from django.utils.dateparse import parse_date
from inventory.models import Product,Pclass
from accounts.models import Account
from .services import post_invoice, post_payment,post_sales_receipt, generate_unique_ref_no, parse_date_flexible, status_for_invoice, _payment_prefill_rows, _delete_existing_payment_journal_entries,_coerce_decimal, delete_sales_receipt_journal



# sales analytics
def _invoice_analytics():
    today = timezone.localdate()
    # Annotate each invoice with decimal-safe totals
    inv = (
        Newinvoice.objects
        .annotate(
            total_due_dec=Cast("total_due", DecimalField(max_digits=18, decimal_places=2)),
            total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00"))),
        )
        .annotate(
            outstanding=Cast(F("total_due_dec") - F("total_paid"), DecimalField(max_digits=18, decimal_places=2))
        )
    )

    # Fully paid (cleared): outstanding <= 0
    paid_qs = inv.filter(outstanding__lte=0)
    paid = paid_qs.aggregate(
        amount=Coalesce(Sum("total_paid"), Value(Decimal("0.00"))),
        count=Coalesce(Sum(Value(1)), Value(0))
    )

    # Unpaid (not overdue): outstanding > 0 and (no due_date or due_date >= today)
    unpaid_qs = inv.filter(outstanding__gt=0).filter(Q(due_date__isnull=True) | Q(due_date__gte=today))
    unpaid = unpaid_qs.aggregate(
        amount=Coalesce(Sum("outstanding"), Value(Decimal("0.00"))),
        count=Coalesce(Sum(Value(1)), Value(0))
    )

    # Overdue: outstanding > 0 and due_date < today
    overdue_qs = inv.filter(outstanding__gt=0, due_date__lt=today)
    overdue = overdue_qs.aggregate(
        amount=Coalesce(Sum("outstanding"), Value(Decimal("0.00"))),
        count=Coalesce(Sum(Value(1)), Value(0))
    )

    return {
        "paid_amount":   paid["amount"]   or Decimal("0.00"),
        "paid_count":    int(paid["count"] or 0),
        "unpaid_amount": unpaid["amount"] or Decimal("0.00"),
        "unpaid_count":  int(unpaid["count"] or 0),
        "over_amount":   overdue["amount"] or Decimal("0.00"),
        "over_count":    int(overdue["count"] or 0),
    }

# sales view
def sales(request):
    products = Product.objects.all()

    # You already use this:
    invoices = Newinvoice.objects.all().prefetch_related("invoiceitem_set")
    inv_analytics = _invoice_analytics()

    rows = []

    # ---- Invoices ----
    inv_qs = (
        Newinvoice.objects
        .select_related("customer")
        .annotate(
            total_due_dec=Cast("total_due", DecimalField(max_digits=18, decimal_places=2)),
            total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00"))),
        )
        .order_by("-date_created", "-id")
    )
    for inv in inv_qs:
        total = inv.total_due_dec or Decimal("0")
        paid  = inv.total_paid or Decimal("0")
        bal   = max(total - paid, Decimal("0"))
        status = status_for_invoice(inv, total, paid, bal)  # you already have this helper

        rows.append({
            "date": getattr(inv, "date_created", None),
            "type": "Invoice",
            "no": f"INV-{inv.id:04d}",
            "customer": inv.customer.customer_name if inv.customer_id else "",
            "memo": (inv.memo or inv.description or "")[:140],
            "amount": total,
            "status": status,
            "edit_url":  reverse("sales:edit-invoice", args=[inv.id]),
            "view_url":  reverse("sales:invoice-detail", args=[inv.id]),
            "print_url": reverse("sales:invoice-print", args=[inv.id]),
        })

    # ---- Payments ----
    pay_qs = (
        Payment.objects
        .select_related("customer", "deposit_to")
        .annotate(
            applied_total=Coalesce(Sum("applied_invoices__amount_paid"), Value(Decimal("0.00"))),
        )
        .order_by("-payment_date", "-id")
    )
    for p in pay_qs:
        rows.append({
            "date": p.payment_date,
            "type": "Payment",
            "no": (p.reference_no or f"{p.id:04d}"),
            "customer": p.customer.customer_name if p.customer_id else "",
            "memo": (p.memo or "")[:140],
            "amount": p.applied_total or Decimal("0"),
            "status": "Closed" if (p.applied_total or 0) > 0 else "Unapplied",
            "edit_url":  reverse("sales:payment-edit", args=[p.id]),
            "view_url":  reverse("sales:payment-detail", args=[p.id]),
            "print_url": reverse("sales:payment-print", args=[p.id]),
        })

    # ---- Sales Receipts ----
    sr_qs = (
        SalesReceipt.objects
        .select_related("customer", "deposit_to")
        .annotate(
            total_amount_dec=Cast("total_amount", DecimalField(max_digits=18, decimal_places=2)),
            amount_paid_dec=Cast(Coalesce(F("amount_paid"), Value(Decimal("0.00"))),
                                 DecimalField(max_digits=18, decimal_places=2)),
        )
        .order_by("-receipt_date", "-id")
    )
    for r in sr_qs:
        total = r.total_amount_dec or Decimal("0")
        paid  = r.amount_paid_dec or Decimal("0")
        bal   = max(total - paid, Decimal("0"))
        status = _receipt_status(r)  # you already have this helper

        rows.append({
            "date": r.receipt_date,
            "type": "Sales Receipt",
            "no": (r.reference_no or f"{r.id:04d}"),
            "customer": r.customer.customer_name if r.customer_id else "",
            "memo": (r.memo or "")[:140],
            "amount": total,
            "status": status,
            "edit_url":  reverse("sales:receipt-edit", args=[r.id]),
            "view_url":  reverse("sales:receipt-detail", args=[r.id]),
            "print_url": reverse("sales:receipt-print", args=[r.id]),
        })

    # sort newest first
    rows.sort(key=lambda x: (x["date"] or 0, x["type"]), reverse=True)

    return render(
        request,
        "Sales.html",
        {
            "products": products,
            "invoices": invoices,
            "inv_analytics": inv_analytics,
            "sales_rows": rows,   # <-- pass to template
        },
    )
# invoice form view

def get_product_details(request, pk):
    try:
        product = Product.objects.get(pk=pk)
        data = {
            "id": product.id,
            "name": product.name,
            "sales_price": str(product.sales_price or 0),
            "taxable": product.taxable,
        }
        return JsonResponse(data)
    except Product.DoesNotExist:
        return JsonResponse({"error": "Product not found"}, status=404)



# working on the invoice 
def parse_date_flexible(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None  # if nothing matched

def add_invoice(request):
    if request.method == "POST":
        # Parse customer and invoice info
        
        date_created = request.POST.get("date_created")
        due_date = request.POST.get("due_date")
        customer_id = request.POST.get("customer")
        customer=None
        if customer_id:
            try:
                customer = Newcustomer.objects.get(pk=customer_id)
            except Newcustomer.DoesNotExist:
                customer=None
        email = request.POST.get("email")
        billing_address = request.POST.get("billing_address")
        shipping_address = request.POST.get("shipping_address")
        terms = request.POST.get("terms")
        sales_rep = request.POST.get("sales_rep")        
        # lclass = request.POST.get("lclass")

        class_field_id = request.POST.get("class_field")
        class_field=None
        if class_field_id:
            try:
                class_field = Pclass.objects.get(pk=class_field_id)
            except Pclass.DoesNotExist:
                class_field=None

        tags = request.POST.get("tags")
        po_num = request.POST.get("po_num")
        memo = request.POST.get("memo")
        customs_notes = request.POST.get("customs_notes")
        subtotal = Decimal(request.POST.get("subtotal") or 0)
        total_discount = Decimal(request.POST.get("total_discount") or 0)
        shipping_fee = Decimal(request.POST.get("shipping_fee") or 0)
        total_due = Decimal(request.POST.get("total_due") or 0)
        # terms and due date
        raw_date_created = request.POST.get("date_created") or ""
        raw_due_date    = request.POST.get("due_date") or ""
        terms           = (request.POST.get("terms") or "").strip()

        # parse to date objects (works for ISO and dd/mm/yyyy)
        created_dt = parse_date_flexible(raw_date_created)
        due_dt     = parse_date_flexible(raw_due_date)

        # Terms → days mapping
        TERMS_DAYS = {
            "due_on_receipt": 0,
            "one_day": 1,
            "two_days": 2,
            "net_7": 7,
            "net_15": 15,
            "net_30": 30,
            "net_60": 60,
            "credit_limit": 27,
            "credit_allowance": 29,
        }

        # if user didn't enter a due date, compute it from terms + created date
        if not due_dt and created_dt and terms in TERMS_DAYS:
            due_dt = created_dt + timedelta(days=TERMS_DAYS[terms])        # Invoice totals
        total_vat = Decimal("0")
        

        # Create invoice
        invoice = Newinvoice.objects.create(
            customer=customer,
            email=email,
            date_created=created_dt,
            due_date=due_dt,
            billing_address=billing_address,
            shipping_address=shipping_address,
            class_field=class_field,
            terms=terms,
            sales_rep=sales_rep,
            tags=tags,
            po_num=po_num,
            memo=memo,
            customs_notes=customs_notes,
            subtotal=subtotal,
            total_discount=total_discount,
            total_vat=total_vat,  # will update after items
            shipping_fee=shipping_fee,
            total_due=total_due,  # will update later
        )

        # ✅ Line items (loop through arrays from form)
        products = request.POST.getlist("product[]")
        descriptions = request.POST.getlist("description[]")
        qtys = request.POST.getlist("qty[]")
        rates = request.POST.getlist("unit_price[]")
        amounts = request.POST.getlist("amount[]")  # fixed typo
        vats = request.POST.getlist("vat[]")
        discount_nums = request.POST.getlist("discount_num[]")
        discount_amounts = request.POST.getlist("discount_amount[]")

        for i in range(len(products)):
            if not products[i]:
                continue

            product = get_object_or_404(Product, pk=products[i])

            InvoiceItem.objects.create(
            invoice=invoice,
            product=product,
            description=descriptions[i] if i < len(descriptions) else "",
            qty=Decimal(qtys[i] or "0") if i < len(qtys) else Decimal("0"),
            unit_price=Decimal(rates[i] or "0") if i < len(rates) else Decimal("0"),
            amount=Decimal(amounts[i] or "0") if i < len(amounts) else Decimal("0"),
            vat=Decimal(vats[i] or "0") if i < len(vats) else Decimal("0"),
            discount_num=Decimal(discount_nums[i] or "0") if i < len(discount_nums) else Decimal("0"),
            discount_amount=Decimal(discount_amounts[i] or "0") if i < len(discount_amounts) else Decimal("0"),
            )
        # Apply discount and shipping
        total_due = (subtotal-total_discount)+shipping_fee+total_vat

        # Update invoice totals
        invoice.total_vat = total_vat
        invoice.total_due = total_due
        invoice.save()
        # calling the post invoice which affects the COA
        post_invoice(invoice)
        # Decide redirect
        save_action = request.POST.get("save_action")
        if save_action == "save&new":
            return redirect("sales:add-invoice")
        elif save_action == "save&close":
            return redirect("sales:sales")

        return redirect("sales:add-invoice")
    products=Product.objects.all()
    customers = Newcustomer.objects.all()
    classes = Pclass.objects.all()
    # working on the id
    last_invoice = Newinvoice.objects.order_by('-id').first()
    next_id = 1 if not last_invoice else last_invoice.id + 1
    next_invoice_id = f"{next_id:03d}"

    return render(request, "invoice_form.html", {
        "customers": customers,
        "classes": classes,
        "products": products,
        "next_invoice_id":next_invoice_id,
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
    
#  invoice list
def invoice_list(request):
    # Pull amounts once (DB-side) to avoid N+1 loops
    invoices_qs = (
        Newinvoice.objects
        .select_related("customer")
        .annotate(
            total_due_dec=Cast(F("total_due"), DecimalField(max_digits=18, decimal_places=2)),
            total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00")))
        )
        .order_by("-date_created", "-id")
    )

    invoices = []
    for inv in invoices_qs:
        total_due  = inv.total_due_dec or Decimal("0")
        total_paid = inv.total_paid or Decimal("0")
        balance    = max(total_due - total_paid, Decimal("0"))

        # ✅ one unified status string everywhere
        inv.status = status_for_invoice(inv, total_due, total_paid, balance)

        invoices.append(inv)

    customers = Newcustomer.objects.all()
    return render(request, "invoice_lists.html", {
        "invoices": invoices,
        "customers": customers,
    })

# ednd
def full_invoice_details(request):
    invoices=Newinvoice.objects.all()
    customers=Newcustomer.objects.all()
    return render(request, 'full_invoice_details.html',{
        'invoices':invoices,
        'customers':customers
    })
# edit and view  views 

def invoice_detail(request, pk: int):
    inv = get_object_or_404(
        Newinvoice.objects.select_related("customer", "class_field"),
        pk=pk
    )

    agg = (
        Newinvoice.objects.filter(pk=pk)
        .annotate(
            total_due_dec=Cast("total_due", DecimalField(max_digits=18, decimal_places=2)),
            total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00"))),
        )
        .values("total_due_dec", "total_paid")
        .first()
    ) or {"total_due_dec": Decimal("0"), "total_paid": Decimal("0")}

    total_due = agg["total_due_dec"] or Decimal("0")
    total_paid = agg["total_paid"] or Decimal("0")
    balance   = max(total_due - total_paid, Decimal("0"))

    # status (same rules you already use in the list)
    today = date.today()
    overdue_days = (today - inv.due_date).days if inv.due_date and balance > 0 and today > inv.due_date else None

    deposited = False
    if total_due > 0 and balance == 0:
        aps = inv.payments_applied.select_related("payment__deposit_to").all()
        if aps:
            def is_bankish(acc):
                if not acc: return False
                at = (acc.account_type or "").lower()
                dt = (acc.detail_type or "").lower()
                return at in ("bank", "cash and cash equivalents", "cash_equiv", "cash & cash equivalents") or "bank" in dt
            deposited = all(is_bankish(pi.payment.deposit_to) for pi in aps if pi.payment)

    if total_due == 0:
        status_text = "No amount"
    elif balance == 0:
        status_text = "Deposited" if deposited else "Paid"
    else:
        if overdue_days:
            status_text = f"Overdue {overdue_days} days"
            if total_paid > 0:
                status_text += f" — Partially paid, {balance:,.0f} due"
            else:
                status_text += f" — {balance:,.0f} due"
        elif inv.due_date and inv.due_date == today and balance > 0:
            status_text = f"Due today — {balance:,.0f} due"
        else:
            status_text = f"Partially paid, {balance:,.0f} due" if total_paid > 0 else f"{balance:,.0f} due"

    items = InvoiceItem.objects.filter(invoice=inv).select_related("product").order_by("id")
    payments = (
        PaymentInvoice.objects
        .filter(invoice=inv)
        .select_related("payment", "payment__deposit_to")
        .order_by("-payment__payment_date", "-id")
    )

    payment_rows = [{
        "date": p.payment.payment_date,
        "ref": p.payment.reference_no,
        "method": (p.payment.payment_method or "").replace("_", " ").title(),
        "deposit_to": p.payment.deposit_to.account_name if p.payment.deposit_to else "",
        "amount": p.amount_paid,
    } for p in payments]

    return render(request, "invoice_detail.html", {
        "inv": inv,
        "items": items,
        "status_text": status_text,
        "total_due": total_due,
        "total_paid": total_paid,
        "balance": balance,
        "payment_rows": payment_rows,
    })

# edit view
def parse_date_flexible(s: str | None):
    """Accept 'YYYY-MM-DD', 'dd/mm/YYYY', 'dd-mm-YYYY', 'mm/dd/YYYY' -> date, or None."""
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None

TERMS_DAYS = {
    "due_on_receipt": 0, "one_day": 1, "two_days": 2, "net_7": 7,
    "net_15": 15, "net_30": 30, "net_60": 60,
    "credit_limit": 27, "credit_allowance": 29,
}


@transaction.atomic
def edit_invoice(request, pk: int):
    """
    Edit an invoice:
      - GET: prefill your existing invoice_form.html
      - POST: update header + replace line items, recompute totals on the server
    """
    inv = get_object_or_404(
        Newinvoice.objects.select_related("customer", "class_field"),
        pk=pk
    )

    if request.method == "POST":
        # ----- Header fields -----
        customer_id   = request.POST.get("customer")
        email         = request.POST.get("email")
        billing_addr  = request.POST.get("billing_address")
        shipping_addr = request.POST.get("shipping_address")
        terms         = (request.POST.get("terms") or "").strip()
        sales_rep     = request.POST.get("sales_rep")
        class_id      = request.POST.get("class_field")
        tags          = request.POST.get("tags")
        po_num        = request.POST.get("po_number") or request.POST.get("po_num")
        memo          = request.POST.get("memo")
        customs_notes = request.POST.get("customs_notes")

        customer   = Newcustomer.objects.filter(pk=customer_id).first() if customer_id else None
        class_field = Pclass.objects.filter(pk=class_id).first() if class_id else None

        created_dt = parse_date_flexible(request.POST.get("date_created"))
        due_dt     = parse_date_flexible(request.POST.get("due_date"))
        if not due_dt and created_dt and terms in TERMS_DAYS:
            due_dt = created_dt + timedelta(days=TERMS_DAYS[terms])

        # We will recompute totals from lines; only shipping is taken from POST
        shipping_fee = Decimal(request.POST.get("shipping_fee") or 0)

        # ----- Replace line items & recompute totals (authoritative) -----
        InvoiceItem.objects.filter(invoice=inv).delete()

        products       = request.POST.getlist("product[]")
        descriptions   = request.POST.getlist("description[]")
        qtys           = request.POST.getlist("qty[]")
        rates          = request.POST.getlist("unit_price[]")
        discount_percs = request.POST.getlist("discount_num[]")

        line_rows: list[InvoiceItem] = []

        subtotal       = Decimal("0.00")
        total_discount = Decimal("0.00")
        total_vat      = Decimal("0.00")

        for i in range(len(products)):
            if not products[i]:
                continue

            product = get_object_or_404(Product, pk=products[i])

            desc = descriptions[i] if i < len(descriptions) else ""
            qty  = Decimal((qtys[i] or "0").strip() if i < len(qtys) else "0")
            rate = Decimal((rates[i] or "0").strip() if i < len(rates) else "0")
            dpc  = Decimal((discount_percs[i] or "0").strip() if i < len(discount_percs) else "0")

            # Base line amount
            line_amount = (qty * rate).quantize(Decimal("0.01"))

            # Discount amount (% of line amount)
            line_discount_amt = (line_amount * dpc / Decimal("100")).quantize(Decimal("0.01"))

            # VAT: keep same logic as create (VAT on pre-discount amount).
            # If you want VAT after discount, change base to (line_amount - line_discount_amt).
            if getattr(product, "taxable", False):
                line_vat = (line_amount * Decimal("0.18")).quantize(Decimal("0.01"))
            else:
                line_vat = Decimal("0.00")

            subtotal       += line_amount
            total_discount += line_discount_amt
            total_vat      += line_vat

            line_rows.append(InvoiceItem(
                invoice=inv,
                product=product,
                description=desc,
                qty=qty,
                unit_price=rate,
                amount=line_amount,          # store pre-discount line amount (matches your create flow)
                vat=line_vat,
                discount_num=dpc,
                discount_amount=line_discount_amt,
            ))

        if line_rows:
            InvoiceItem.objects.bulk_create(line_rows)

        # Final totals = (subtotal - discount) + VAT + shipping
        inv.customer        = customer
        inv.email           = email
        inv.date_created    = created_dt
        inv.due_date        = due_dt
        inv.billing_address = billing_addr
        inv.shipping_address= shipping_addr
        inv.class_field     = class_field
        inv.terms           = terms
        inv.sales_rep       = sales_rep
        inv.tags            = tags
        inv.po_num          = po_num
        inv.memo            = memo
        inv.customs_notes   = customs_notes

        inv.subtotal        = subtotal
        inv.total_discount  = total_discount
        inv.total_vat       = total_vat
        inv.shipping_fee    = shipping_fee
        inv.total_due       = (subtotal - total_discount + total_vat + shipping_fee).quantize(Decimal("0.01"))

        inv.save()

        # NOTE: Avoid re-posting to the journal here unless you have a revisioning strategy.
        # If you must journal changes, add a safe update in your accounting layer.

        return redirect("sales:invoice-detail", pk=inv.pk)

    # ----- GET: prefill form -----
    products  = Product.objects.all()
    customers = Newcustomer.objects.all()
    classes   = Pclass.objects.all()
    items     = InvoiceItem.objects.filter(invoice=inv).select_related("product").order_by("id")

    return render(request, "invoice_form.html", {
        "edit_mode": True,
        "inv": inv,
        "items": items,
        "products": products,
        "customers": customers,
        "classes": classes,
        "next_invoice_id": f"{inv.id:03d}",  # keep your visual header format
    })
# invoice print view
def invoice_print(request, pk: int):
    inv = get_object_or_404(
        Newinvoice.objects.select_related("customer", "class_field"),
        pk=pk
    )

    agg = (
        Newinvoice.objects.filter(pk=pk)
        .annotate(
            total_due_dec=Cast("total_due", DecimalField(max_digits=18, decimal_places=2)),
            total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00"))),
        )
        .values("total_due_dec", "total_paid")
        .first()
    ) or {"total_due_dec": Decimal("0"), "total_paid": Decimal("0")}

    total_due = agg["total_due_dec"] or Decimal("0")
    total_paid = agg["total_paid"] or Decimal("0")
    balance   = max(total_due - total_paid, Decimal("0"))
    status_text = status_for_invoice(inv, total_due, total_paid, balance)

    items = InvoiceItem.objects.filter(invoice=inv).select_related("product").order_by("id")

    payments = (
        PaymentInvoice.objects
        .filter(invoice=inv)
        .select_related("payment", "payment__deposit_to")
        .order_by("-payment__payment_date", "-id")
    )
    payment_rows = [{
        "date": p.payment.payment_date,
        "ref": p.payment.reference_no,
        "method": (p.payment.payment_method or "").replace("_", " ").title(),
        "deposit_to": p.payment.deposit_to.account_name if p.payment.deposit_to else "",
        "amount": p.amount_paid,
    } for p in payments]

    # Optional: company/org details (replace these with your real ones or pull from a Company model)
    org = {
        "name": "Sowa Accountants Ltd",
        "address": "Plot 123, Kampala Road, Kampala",
        "phone": "+256 700 000 000",
        "email": "accounts@sowaf.co.ug",
        "website": "www.sowaf.co.ug",
        "logo_url": request.build_absolute_uri(static("sowaf/images/yo-logo.png")),
    }

    return render(request, "invoice_print.html", {
        "inv": inv,
        "items": items,
        "status_text": status_text,
        "total_due": total_due,
        "total_paid": total_paid,
        "balance": balance,
        "payment_rows": payment_rows,
        "org": org,
    })


# receipt form view

def add_receipt(request):
    
    return render(request, 'receipt_form.html', {})
# receive payment form view

# Allowed account types for "Deposit To"
CASH_EQ_DETAIL_CHOICES = [
    "Bank", "Cash on hand", "Petty Cash", "Undeposited Funds", "Mobile Money", "MOMO", "Wallet"
]

def deposit_accounts_qs():
    """
    Returns accounts that should appear in 'Deposit To':
    - Account Type == 'Cash and Cash Equivalents' (or 'CASH_EQUIV')
    - OR Detail Type includes typical cash/bank items (e.g., 'Bank', 'Cash on hand', etc.)
    - Also includes Account Type == 'Bank' if you ever store it that way.
    """
    return Account.objects.filter(
        Q(account_type__iexact="Cash and Cash Equivalents") |
        Q(account_type__iexact="CASH_EQUIV") |
        Q(account_type__iexact="Bank") |
        Q(detail_type__in=CASH_EQ_DETAIL_CHOICES)
    ).filter(is_active=True).order_by("account_name", "account_number")
# payments
def receive_payment_view(request):
    customers = Newcustomer.objects.order_by("customer_name")
    accounts = deposit_accounts_qs()   # for the dropdown

    if request.method == "POST":
        customer_id = (request.POST.get("customer") or "").strip()
        payment_date = parse_date(request.POST.get("payment_date") or "")
        payment_method = (request.POST.get("payment_method") or "cash").strip()
        deposit_to_id = (request.POST.get("deposit_to") or "").strip()
        reference_no = (request.POST.get("reference_no") or "").strip()
        tags = (request.POST.get("tags") or "").strip()
        memo = (request.POST.get("memo") or "").strip()

        # resolve & validate deposit account **only from allowed set**
        deposit_account = accounts.filter(id=deposit_to_id).first() if deposit_to_id else None

        # ensure we have a valid 8-digit numeric ref; if not, generate one
        if not (len(reference_no) == 8 and reference_no.isdigit()):
            reference_no = generate_unique_ref_no()
        # guard against the (very rare) race where the same ref got used meanwhile
        if Payment.objects.filter(reference_no=reference_no).exists():
            reference_no = generate_unique_ref_no()

        if not (customer_id.isdigit() and payment_date and deposit_account):
            # return the same prefilled ref back to the form so it stays visible
            return render(request, "receive_payment.html", {
                "customers": customers,
                "accounts": accounts,
                "reference_no": reference_no,
                "form_error": "Please select a customer, a valid Bank/Cash & Cash Equivalents account, and a date.",
            })

        customer = get_object_or_404(Newcustomer, pk=int(customer_id))

        # collect allocations like amount_paid_<invoice_id>
        allocations = []
        for key, val in request.POST.items():
            if key.startswith("amount_paid_"):
                inv_id = key.split("_")[-1]
                if inv_id.isdigit():
                    s = (val or "").strip()
                    if s:
                        amt = Decimal(s)
                        if amt > 0:
                            allocations.append((int(inv_id), amt))

        if not allocations:
            return render(request, "receive_payment.html", {
                "customers": customers, "accounts": accounts, "reference_no": reference_no,
                "form_error": "Enter at least one positive Amount to Apply.",
            })

        # validate vs outstanding & save
        with transaction.atomic():
            balances = (
                Newinvoice.objects.filter(id__in=[i for i, _ in allocations])
                .annotate(
                    total_due_dec=Cast(F("total_due"), DecimalField(max_digits=18, decimal_places=2)),
                    total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00")))
                )
                .annotate(outstanding_balance=F("total_due_dec") - F("total_paid"))
                .values_list("id", "outstanding_balance")
            )
            balance_map = {iid: bal for iid, bal in balances}

            for invoice_id, amount in allocations:
                max_allowed = balance_map.get(invoice_id)
                if max_allowed is None or amount > max_allowed:
                    return render(request, "receive_payment.html", {
                        "customers": customers, "accounts": accounts, "reference_no": reference_no,
                        "form_error": f"Allocation {amount} exceeds outstanding balance {max_allowed} on invoice {invoice_id}.",
                    })

            payment = Payment.objects.create(
                customer=customer,
                payment_date=payment_date,
                payment_method=payment_method,
                deposit_to=deposit_account,
                reference_no=reference_no, 
                tags=tags,
                memo=memo,
            )

            PaymentInvoice.objects.bulk_create([
                PaymentInvoice(payment=payment, invoice_id=inv_id, amount_paid=amt)
                for inv_id, amt in allocations
            ])

        post_payment(payment)
        return redirect(f"{request.path}?ok=1")

    # GET: pre-generate and pass to template so it’s visible immediately
    reference_no = generate_unique_ref_no()
    return render(request, "receive_payment.html", {
        "customers": customers,
        "accounts": accounts,
        "reference_no": reference_no,   # <-- make sure the template uses this
    })

def outstanding_invoices_api(request):
    cid = request.GET.get("customer")
    if not cid or cid == "add_new":
        return JsonResponse({"invoices": []})
    try:
        cid_int = int(cid)
    except ValueError:
        return JsonResponse({"invoices": []})

    customer = get_object_or_404(Newcustomer, pk=cid_int)

    qs = (
        Newinvoice.objects
        .filter(customer=customer)
        .annotate(
            total_due_dec=Cast(F("total_due"), DecimalField(max_digits=18, decimal_places=2)),
            total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00")))
        )
        # working on the balance
        .annotate(outstanding_balance=F("total_due_dec") - F("total_paid"))
        .filter(outstanding_balance__gt=0)
        .order_by("-date_created")
        # return dicts to avoid setting attributes on model instances
        .values("id", "date_created", "due_date", "total_due", "outstanding_balance")
    )

    data = []
    for row in qs:
        dc = row["date_created"]
        dd = row["due_date"]
        data.append({
            "id": row["id"],
            "date_created": dc.isoformat() if dc else None,
            "due_date": dd.isoformat() if dd else None,
            "total_due": str(row["total_due"]),                 # float -> string for JSON
            "balance": str(row["outstanding_balance"]),         # keep JSON key as "balance"
        })
    return JsonResponse({"invoices": data})

# payment lists
def payments_list(request):
    payments = (
        Payment.objects
        .select_related("customer", "deposit_to")
        .prefetch_related("applied_invoices__invoice")
        .order_by("-payment_date", "-id")
    )

    # collect invoice ids appearing in payment lines
    invoice_ids = set()
    for p in payments:
        for pli in p.applied_invoices.all():
            invoice_ids.add(pli.invoice_id)

    # total paid to date per invoice
    totals = (
        PaymentInvoice.objects
        .filter(invoice_id__in=invoice_ids)
        .values("invoice_id")
        .annotate(total_paid=Sum("amount_paid"))
    )
    total_paid_map = {row["invoice_id"]: row["total_paid"] for row in totals}

    # fetch invoice objects
    invoices_by_id = Newinvoice.objects.in_bulk(invoice_ids)

    rows = []
    for p in payments:
        line_rows = []
        for pli in p.applied_invoices.all():
            inv = invoices_by_id.get(pli.invoice_id)
            if not inv:
                continue
            total_due = Decimal(str(inv.total_due or "0"))
            amount_applied = pli.amount_paid
            remaining_this_payment = total_due - amount_applied
            outstanding_now = total_due - (total_paid_map.get(pli.invoice_id) or Decimal("0"))

            line_rows.append({
                "invoice": inv,
                "amount_applied": amount_applied,
                "total_due": total_due,
                "remaining_this_payment": remaining_this_payment,
                "outstanding_now": outstanding_now,
            })

        # precompute section totals so the template stays simple
        applied_total = sum((lr["amount_applied"] for lr in line_rows), Decimal("0"))
        remaining_total_this_payment = sum((lr["remaining_this_payment"] for lr in line_rows), Decimal("0"))
        outstanding_total_now = sum((lr["outstanding_now"] for lr in line_rows), Decimal("0"))

        rows.append({
            "payment": p,
            "lines": line_rows,
            "applied_total": applied_total,
            "remaining_total_this_payment": remaining_total_this_payment,
            "outstanding_total_now": outstanding_total_now,
        })

    return render(request, "payments_list.html", {"rows": rows})
# individual payment
def payment_detail(request, pk: int):
    payment = get_object_or_404(
        Payment.objects.select_related("customer", "deposit_to"),
        pk=pk
    )
    group = _payment_prefill_rows(payment)
    return render(request, "payment_detail.html", {"group": group, "payment":payment})

# edit view 
@transaction.atomic
def payment_edit(request, pk: int):
    """
    Edit a payment using the SAME template receive_payment.html,
    fully prefilled (customer, date, method, deposit_to, reference, tags, memo, and invoice allocations).
    """
    payment = get_object_or_404(
        Payment.objects.select_related("customer", "deposit_to"),
        pk=pk
    )
    customers = Newcustomer.objects.order_by("customer_name")
    accounts  = deposit_accounts_qs()

    if request.method == "POST":
        customer_id   = (request.POST.get("customer") or "").strip()
        payment_date  = parse_date(request.POST.get("payment_date") or "")
        payment_method= (request.POST.get("payment_method") or "cash").strip()
        deposit_to_id = (request.POST.get("deposit_to") or "").strip()
        reference_no  = (request.POST.get("reference_no") or "").strip()
        tags          = (request.POST.get("tags") or "").strip()
        memo          = (request.POST.get("memo") or "").strip()

        # validate & resolve
        if not (customer_id.isdigit() and payment_date and deposit_to_id.isdigit()):
            return render(request, "receive_payment.html", {
                "customers": customers, "accounts": accounts,
                "payment": payment,
                "reference_no": payment.reference_no or generate_unique_ref_no(),
                "prefill_rows": _payment_prefill_rows(payment),
                "edit_mode": True,
                "form_error": "Please select a customer, a valid Bank/Cash & Cash Equivalents account, and a date.",
            })

        customer = get_object_or_404(Newcustomer, pk=int(customer_id))
        deposit_account = get_object_or_404(accounts, pk=int(deposit_to_id))

        # collect incoming allocations
        allocations = []
        for key, val in request.POST.items():
            if not key.startswith("amount_paid_"):
                continue
            inv_id_part = key.split("_")[-1]
            if not inv_id_part.isdigit():
                continue
            raw = (val or "").strip()
            if not raw:
                continue
            amt = Decimal(raw)
            if amt > 0:
                allocations.append((int(inv_id_part), amt))

        if not allocations:
            return render(request, "receive_payment.html", {
                "customers": customers, "accounts": accounts,
                "payment": payment,
                "reference_no": payment.reference_no or generate_unique_ref_no(),
                "prefill_rows": _payment_prefill_rows(payment),
                "edit_mode": True,
                "form_error": "Enter at least one positive Amount to Apply.",
            })

        # VALIDATION against outstanding, but allow reusing this payment’s previous amounts
        # Build map of this payment's previous allocations
        prev_alloc_qs = PaymentInvoice.objects.filter(payment=payment).values_list('invoice_id', 'amount_paid')
        prev_map = {}
        for iid, amt in prev_alloc_qs:
            prev_map[iid] = prev_map.get(iid, Decimal("0.00")) + Decimal(amt or 0)

        invoice_ids = [i for i, _ in allocations]
        balances = (
            Newinvoice.objects.filter(id__in=invoice_ids)
            .annotate(
                total_due_dec=Cast(F("total_due"), DecimalField(max_digits=18, decimal_places=2)),
                total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00")))
            )
            .annotate(outstanding_balance=F("total_due_dec") - F("total_paid"))
            .values_list("id", "outstanding_balance")
        )
        balance_map = {iid: Decimal(bal) for iid, bal in balances}

        # Allow new_amount <= outstanding + previously_applied_by_this_payment
        for invoice_id, new_amt in allocations:
            allowed = balance_map.get(invoice_id, Decimal("0.00")) + prev_map.get(invoice_id, Decimal("0.00"))
            if new_amt > allowed:
                return render(request, "receive_payment.html", {
                    "customers": customers, "accounts": accounts,
                    "payment": payment,
                    "reference_no": payment.reference_no or generate_unique_ref_no(),
                    "prefill_rows": _payment_prefill_rows(payment),
                    "edit_mode": True,
                    "form_error": f"Allocation {new_amt} exceeds allowed {allowed} on invoice {invoice_id}.",
                })

        # save header
        payment.customer      = customer
        payment.payment_date  = payment_date
        payment.payment_method= payment_method
        payment.deposit_to    = deposit_account
        payment.reference_no  = reference_no if reference_no else payment.reference_no
        payment.tags          = tags
        payment.memo          = memo
        payment.save()

        # replace allocations
        PaymentInvoice.objects.filter(payment=payment).delete()
        PaymentInvoice.objects.bulk_create([
            PaymentInvoice(payment=payment, invoice_id=inv_id, amount_paid=amt)
            for inv_id, amt in allocations
        ])

        # re-post journal: delete existing then re-post cleanly
        _delete_existing_payment_journal_entries(payment)
        post_payment(payment)

        return redirect('sales:payment-detail', pk=payment.pk)

    # GET → prefill
    context = {
        "customers": customers,
        "accounts": accounts,
        "payment": payment,
        "reference_no": payment.reference_no or generate_unique_ref_no(),
        "prefill_rows": _payment_prefill_rows(payment),
        "edit_mode": True,
    }
    return render(request, "receive_payment.html", context)
# payment printout  
def _lines_for_payment(payment: Payment):
    """
    Build per-invoice rows for this payment:
      - invoice basic info
      - total_due (as Decimal)
      - amount_applied (this payment)
      - previously_paid (all payments with id < this payment.id)
      - remaining_this_payment
      - outstanding_now
    """
    # ids of invoices touched by this payment
    ids = list(
        PaymentInvoice.objects.filter(payment=payment).values_list("invoice_id", flat=True)
    )
    if not ids:
        return [], Decimal("0.00"), Decimal("0.00"), Decimal("0.00")

    # how much each of those invoices got from THIS payment
    applied_map = {
        row["invoice_id"]: row["applied"]
        for row in PaymentInvoice.objects.filter(payment=payment)
        .values("invoice_id")
        .annotate(applied=Coalesce(Sum("amount_paid"), Value(Decimal("0.00"))))
    }

    # how much each invoice had before this payment (use id ordering as a stable proxy)
    prev_paid_map = {
        row["invoice_id"]: row["paid_before"]
        for row in PaymentInvoice.objects.filter(
            invoice_id__in=ids,
            payment__id__lt=payment.id
        )
        .values("invoice_id")
        .annotate(paid_before=Coalesce(Sum("amount_paid"), Value(Decimal("0.00"))))
    }

    # pull invoices with their total_due as Decimal
    invoices = (
        Newinvoice.objects.filter(id__in=ids)
        .annotate(total_due_dec=F("total_due"))
        .select_related("customer")
        .order_by("id")
    )

    rows = []
    applied_total = Decimal("0.00")
    remaining_total = Decimal("0.00")
    outstanding_total = Decimal("0.00")

    for inv in invoices:
        total_due = Decimal(str(inv.total_due_dec or "0"))
        applied = Decimal(str(applied_map.get(inv.id, Decimal("0.00"))))
        paid_before = Decimal(str(prev_paid_map.get(inv.id, Decimal("0.00"))))

        remaining_this_payment = max(total_due - paid_before - applied, Decimal("0.00"))
        outstanding_now = max(total_due - (paid_before + applied), Decimal("0.00"))

        rows.append({
            "invoice": inv,
            "date_created": inv.date_created,
            "total_due": total_due,
            "amount_applied": applied,
            "remaining_this_payment": remaining_this_payment,
            "outstanding_now": outstanding_now,
        })

        applied_total += applied
        remaining_total += remaining_this_payment
        outstanding_total += outstanding_now

    return rows, applied_total, remaining_total, outstanding_total


def payment_print(request, pk: int):
    """
    Printable Payment Receipt.
    """
    payment = get_object_or_404(
        Payment.objects.select_related("customer", "deposit_to"),
        pk=pk
    )
    lines, applied_total, remaining_total, outstanding_total = _lines_for_payment(payment)

    # company / branding (replace with your own source if you store company profile elsewhere)
    company = {
        "name": "YoAccountant",
        "address": "Kampala, Uganda",
        "phone": "+256 000 000 000",
        "email": "info@yoaccountant.com",
        "logo_url": request.build_absolute_uri(static("sowaf/images/yo-logo.png")),
    }

    ctx = {
        "payment": payment,
        "lines": lines,
        "applied_total": applied_total,
        "remaining_total": remaining_total,
        "outstanding_total": outstanding_total,
        "company": company,
    }
    return render(request, "payment_print.html", ctx)
# end

# working on the receipt

@transaction.atomic
def sales_receipt_new(request):
    customers = Newcustomer.objects.order_by("customer_name")
    accounts  = deposit_accounts_qs()
    products  = Product.objects.all()

    if request.method == "POST":
        # --- header fields ---
        customer_id    = (request.POST.get("customer") or "").strip()
        receipt_date   = parse_date(request.POST.get("receipt_date") or "")
        payment_method = (request.POST.get("payment_method") or "cash").strip()
        deposit_to_id  = (request.POST.get("deposit_to") or "").strip()
        reference_no   = (request.POST.get("reference_no") or "").strip() or generate_unique_ref_no()
        tags           = (request.POST.get("tags") or "").strip()
        memo           = (request.POST.get("memo") or "").strip()

        if not (customer_id.isdigit() and receipt_date and deposit_to_id.isdigit()):
            return render(request, "receipt_form.html", {
                "customers": customers, "accounts": accounts, "products": products,
                "reference_no": reference_no,
                "form_error": "Please select customer, date and a deposit account.",
            })

        customer   = get_object_or_404(Newcustomer, pk=int(customer_id))
        deposit_to = get_object_or_404(accounts, pk=int(deposit_to_id))
        subtotal        = _coerce_decimal(request.POST.get("subtotal"))              # number
        discount_amount = _coerce_decimal(request.POST.get("discount_amount"))       # number
        shipping_fee    = _coerce_decimal(request.POST.get("shipping"))              # number
        total_amount    = _coerce_decimal(request.POST.get("total"))                 # number (a.k.a. grandTotal)
        amount_paid     = _coerce_decimal(request.POST.get("amount_paid"))           # number
        balance         = total_amount - amount_paid
        if balance < 0:
            balance = Decimal("0.00")

        # ensure 8-digit numeric ref
        if not (len(reference_no) == 8 and reference_no.isdigit()):
            reference_no = generate_unique_ref_no()
        # ultra-rare collision guard across payments/receipts if you like:
        if Payment.objects.filter(reference_no=reference_no).exists() or \
           SalesReceipt.objects.filter(reference_no=reference_no).exists():
            reference_no = generate_unique_ref_no()

        # --- create header ---
        receipt = SalesReceipt.objects.create(
            customer=customer,
            receipt_date=receipt_date,
            payment_method=payment_method,
            deposit_to=deposit_to,
            reference_no=reference_no,
            tags=tags,
            memo=memo,
            subtotal=subtotal,
            total_discount=discount_amount,
            total_vat=Decimal("0.00"),
            shipping_fee=shipping_fee,
            total_amount=total_amount,
            amount_paid=amount_paid,   # <-- save it
            balance=balance,
            )

        # --- lines (use your posted names) ---
        products_ids = request.POST.getlist("product[]")
        descriptions = request.POST.getlist("description[]")
        qtys         = request.POST.getlist("qty[]")
        unit_prices  = request.POST.getlist("unit_price[]")
        line_totals  = request.POST.getlist("line_total[]")  # NOTE: from your form

        bulk = []
        row_count = max(len(descriptions), len(qtys), len(unit_prices), len(line_totals), len(products_ids))
        for i in range(row_count):
            prod_id = products_ids[i] if i < len(products_ids) else None
            product = Product.objects.filter(pk=prod_id).first() if (prod_id and str(prod_id).isdigit()) else None

            desc = descriptions[i] if i < len(descriptions) else ""
            qty  = _coerce_decimal(qtys[i] if i < len(qtys) else "0")
            rate = _coerce_decimal(unit_prices[i] if i < len(unit_prices) else "0")
            amt  = _coerce_decimal(line_totals[i] if i < len(line_totals) else "0")

            # skip completely empty lines
            if not (product or desc or (qty > 0) or (rate > 0) or (amt > 0)):
                continue

            bulk.append(SalesReceiptLine(
                receipt=receipt,
                product=product,
                description=desc,
                qty=qty,
                unit_price=rate,
                amount=amt,
                discount_pct=Decimal("0.00"),
                discount_amt=Decimal("0.00"),
                vat_amt=Decimal("0.00"),
            ))
        if bulk:
            SalesReceiptLine.objects.bulk_create(bulk)

        # --- Post to Journal (cash sale): DR deposit_to, CR Sales Income
        post_sales_receipt(receipt)

        # --- redirects ---
        action = request.POST.get("save_action")
        if action == "save":
            return redirect("sales:sales-receipt-list")
        if action == "save&new":
            return redirect("sales:sales-receipt-new")
        if action == "save&close":
            return redirect("sales:sales-receipt-list")
        return redirect("sales:receipt-detail", pk=receipt.pk)

    # GET: prefill a reference number like your payment page
    reference_no = generate_unique_ref_no()
    return render(request, "receipt_form.html", {
        "customers": customers,
        "accounts": accounts,
        "products": products,
        "reference_no": reference_no,
    })

@transaction.atomic
def sales_receipt_edit(request, pk: int):
    receipt   = get_object_or_404(SalesReceipt.objects.select_related("customer", "deposit_to"), pk=pk)
    customers = Newcustomer.objects.order_by("customer_name")
    accounts  = deposit_accounts_qs()
    products  = Product.objects.all()

    if request.method == "POST":
        customer_id    = (request.POST.get("customer") or "").strip()
        receipt_date   = parse_date(request.POST.get("receipt_date") or "")
        payment_method = (request.POST.get("payment_method") or "cash").strip()
        deposit_to_id  = (request.POST.get("deposit_to") or "").strip()
        reference_no   = (request.POST.get("reference_no") or "").strip() or receipt.reference_no
        tags           = (request.POST.get("tags") or "").strip()
        memo           = (request.POST.get("memo") or "").strip()

        errors = []
        if not (customer_id.isdigit()):
            errors.append("customer")
        if not receipt_date:
            errors.append("date")
        if not (deposit_to_id.isdigit()):
            errors.append("deposit account")
        if errors:
            return render(request, "receipt_form.html", {
                "customers": customers, "accounts": accounts, "products": products,
                "edit_mode": True, "receipt": receipt, "items": receipt.lines.all(),
                "reference_no": reference_no,
                "form_error": "Please select: " + ", ".join(errors) + ".",
            })

        receipt.customer       = get_object_or_404(Newcustomer, pk=int(customer_id))
        receipt.receipt_date   = receipt_date
        receipt.payment_method = payment_method
        receipt.deposit_to     = get_object_or_404(accounts, pk=int(deposit_to_id))
        receipt.reference_no   = reference_no
        receipt.tags           = tags
        receipt.memo           = memo

        # totals (map from form)
        receipt.amount_paid    = _coerce_decimal(request.POST.get("amount_paid"))
        receipt.balance        = (receipt.total_amount - receipt.amount_paid)
        receipt.subtotal       = _coerce_decimal(request.POST.get("subtotal"))
        receipt.total_discount = _coerce_decimal(request.POST.get("discount_amount"))
        receipt.total_vat      = Decimal("0.00")
        receipt.shipping_fee   = _coerce_decimal(request.POST.get("shipping_fee"))
        receipt.total_amount   = _coerce_decimal(request.POST.get("total_amount"))
        receipt.save()

        # replace lines
        SalesReceiptLine.objects.filter(receipt=receipt).delete()

        products_ids = request.POST.getlist("product[]")
        descriptions = request.POST.getlist("description[]")
        qtys         = request.POST.getlist("qty[]")
        unit_prices  = request.POST.getlist("unit_price[]")
        line_totals  = request.POST.getlist("line_total[]")

        bulk = []
        n = max(len(descriptions), len(products_ids), len(qtys), len(unit_prices), len(line_totals))
        for i in range(n):
            prod_id = products_ids[i] if i < len(products_ids) else None
            product = Product.objects.filter(pk=prod_id).first() if (prod_id and str(prod_id).isdigit()) else None

            desc  = descriptions[i] if i < len(descriptions) else ""
            qty   = _coerce_decimal(qtys[i] if i < len(qtys) else "0")
            price = _coerce_decimal(unit_prices[i] if i < len(unit_prices) else "0")
            amt   = _coerce_decimal(line_totals[i] if i < len(line_totals) else "0")

            if not product and not desc and qty == 0 and price == 0 and amt == 0:
                continue

            bulk.append(SalesReceiptLine(
                receipt=receipt,
                product=product,
                description=desc,
                qty=qty,
                unit_price=price,
                amount=amt,
                discount_pct=Decimal("0.00"),
                discount_amt=Decimal("0.00"),
                vat_amt=Decimal("0.00"),
            ))
        if bulk:
            SalesReceiptLine.objects.bulk_create(bulk)

        # re-post journal
        delete_sales_receipt_journal(receipt)
        post_sales_receipt(receipt)

        return redirect("sales:receipt-detail", pk=receipt.pk)

    # GET
    return render(request, "receipt_form.html", {
        "customers": customers,
        "accounts": accounts,
        "products": products,
        "edit_mode": True,
        "receipt": receipt,
        "items": receipt.lines.all(),
        "reference_no": receipt.reference_no or generate_unique_ref_no(),
    })
def sales_receipt_detail(request, pk: int):
    receipt = get_object_or_404(SalesReceipt.objects.select_related("customer", "deposit_to"), pk=pk)
    lines = receipt.lines.select_related("product").all()

    return render(request, "receipt_detail.html", {
        "receipt": receipt,
        "lines": lines,
    })

# receipt lists and printout

def _is_bankish(acc) -> bool:
    if not acc:
        return False
    at = (acc.account_type or "").lower()
    dt = (acc.detail_type or "").lower()
    return (
        at in ("bank", "cash and cash equivalents", "cash_equiv", "cash & cash equivalents")
        or "bank" in dt
    )


def _receipt_status(r: SalesReceipt) -> str:
    """
    Simple, readable status like we did for invoices/payments:
    - Deposited (if fully paid & deposited to a bankish account)
    - Paid (if balance 0 but account not bankish)
    - <balance> due (if balance > 0)
    """
    total = r.total_amount or Decimal("0")
    paid  = getattr(r, "amount_paid", Decimal("0"))
    bal   = getattr(r, "balance", (total - paid))
    if total == 0:
        return "No amount"
    if bal <= 0:
        return "Deposited" if _is_bankish(r.deposit_to) else "Paid"
    return f"{bal:,.0f} due"


def sales_receipt_list(request):
    """
    Receipts table with customer, date, deposit_to, method, totals,
    plus Actions (Edit | View | Print).
    """
    qs = (
        SalesReceipt.objects
        .select_related("customer", "deposit_to")
        .annotate(
            total_amount_dec=Cast("total_amount", DecimalField(max_digits=18, decimal_places=2)),
            amount_paid_dec=Cast(Coalesce(F("amount_paid"), Value(Decimal("0.00"))), DecimalField(max_digits=18, decimal_places=2)),
        )
        .order_by("-receipt_date", "-id")
    )

    rows = []
    for r in qs:
        total   = r.total_amount_dec or Decimal("0")
        paid    = r.amount_paid_dec or Decimal("0")
        balance = getattr(r, "balance", (total - paid))
        if balance is None:
            balance = total - paid
        if balance < 0:
            balance = Decimal("0")

        rows.append({
            "r": r,
            "total": total,
            "paid": paid,
            "balance": balance,
            "status": _receipt_status(r),
        })

    return render(request, "receipt_list.html", {"rows": rows})


def receipt_print(request, pk: int):
    receipt = get_object_or_404(
        SalesReceipt.objects.select_related("customer", "deposit_to"), pk=pk
    )
    lines = receipt.lines.select_related("product").all()

    context = {
        "receipt": receipt,
        "lines": lines,
        # header info (use your real settings if you have them)
        "logo_url": request.build_absolute_uri(static("sowaf/images/yo-logo.png")),
        "company_name": "YoAccountant",
        "company_address": "Kampala, Uganda",
        "company_phone": "+256 700 000 000",
        "company_email": "support@yoaccountant.com",
    }
    return render(request, "receipt_print.html", context)
# end
