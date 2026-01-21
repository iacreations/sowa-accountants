from decimal import Decimal
from datetime import timedelta

from django.db import transaction
from django.utils import timezone

try:
    from dateutil.relativedelta import relativedelta
except Exception:
    relativedelta = None

from .models import (
    RecurringInvoice,
    RecurringInvoiceLine,
    RecurringGeneratedInvoice,
    Newinvoice,
    InvoiceItem,
)

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


def _compute_next_run(current_date, frequency: str, interval: int):
    interval = int(interval or 1)
    if interval < 1:
        interval = 1

    if frequency == "daily":
        return current_date + timedelta(days=interval)

    if frequency == "weekly":
        return current_date + timedelta(days=7 * interval)

    if frequency == "monthly":
        if relativedelta:
            return current_date + relativedelta(months=interval)
        # fallback: naive month add (keeps day if possible)
        y = current_date.year
        m = current_date.month + interval
        while m > 12:
            y += 1
            m -= 12
        d = min(current_date.day, 28)  # safe fallback
        return current_date.replace(year=y, month=m, day=d)

    if frequency == "yearly":
        if relativedelta:
            return current_date + relativedelta(years=interval)
        return current_date.replace(year=current_date.year + interval)

    # default monthly
    if relativedelta:
        return current_date + relativedelta(months=interval)
    return current_date + timedelta(days=30 * interval)


def _compute_due_date(created_date, terms: str):
    if not created_date:
        return None
    days = TERMS_DAYS.get((terms or "").strip())
    if days is None:
        return created_date
    return created_date + timedelta(days=int(days))


@transaction.atomic
def generate_recurring_invoices_for_date(run_date=None, *, apply_audit_fields=None, _post_invoice_to_ledger=None, as_aware_datetime=None):
    """
    Generates invoices for recurring templates due on or before run_date (default today).
    Requires passing your existing helpers from views.py:
      - apply_audit_fields(obj)
      - _post_invoice_to_ledger(invoice)
      - as_aware_datetime(date_or_datetime)
    """
    if run_date is None:
        run_date = timezone.localdate()

    qs = RecurringInvoice.objects.select_related("customer", "class_field").filter(
        is_active=True,
        next_run_date__lte=run_date,
    )

    created = 0
    skipped = 0
    deactivated = 0

    for rec in qs:
        # stop conditions
        if rec.end_date and rec.next_run_date and rec.next_run_date > rec.end_date:
            rec.is_active = False
            rec.save(update_fields=["is_active"])
            deactivated += 1
            continue

        if rec.max_occurrences and rec.occurrences_generated >= rec.max_occurrences:
            rec.is_active = False
            rec.save(update_fields=["is_active"])
            deactivated += 1
            continue

        # prevent duplicates for the same run date
        already = RecurringGeneratedInvoice.objects.filter(recurring=rec, run_date=rec.next_run_date).exists()
        if already:
            # still advance schedule so you don't get stuck
            rec.next_run_date = _compute_next_run(rec.next_run_date, rec.frequency, rec.interval)
            rec.save(update_fields=["next_run_date"])
            skipped += 1
            continue

        # create invoice
        created_dt = rec.next_run_date
        due_dt = _compute_due_date(created_dt, rec.terms)

        invoice = Newinvoice.objects.create(
            customer=rec.customer,
            email=rec.email,
            date_created=as_aware_datetime(created_dt) if as_aware_datetime else None,
            due_date=as_aware_datetime(due_dt) if as_aware_datetime else None,
            billing_address=rec.billing_address,
            shipping_address=rec.shipping_address,
            class_field=rec.class_field,
            terms=rec.terms,
            sales_rep=rec.sales_rep,
            tags=rec.tags,
            po_num=rec.po_num,
            memo=rec.memo,
            customs_notes=rec.customs_notes,
            subtotal=Decimal("0.00"),
            total_discount=Decimal("0.00"),
            total_vat=Decimal("0.00"),
            shipping_fee=Decimal(rec.shipping_fee or 0),
            total_due=Decimal("0.00"),
        )

        subtotal = Decimal("0.00")
        total_discount = Decimal("0.00")
        total_vat = Decimal("0.00")

        lines = RecurringInvoiceLine.objects.select_related("product").filter(recurring=rec).order_by("id")
        item_rows = []

        for ln in lines:
            product = ln.product
            qty = Decimal(ln.qty or 0)
            rate = Decimal(ln.unit_price or 0)
            dpc = Decimal(ln.discount_num or 0)

            line_amount = (qty * rate).quantize(Decimal("0.01"))
            line_discount_amt = (line_amount * dpc / Decimal("100")).quantize(Decimal("0.01"))

            # VAT (18%) if taxable
            if getattr(product, "taxable", False):
                line_vat = (line_amount * Decimal("0.18")).quantize(Decimal("0.01"))
            else:
                line_vat = Decimal("0.00")

            subtotal += line_amount
            total_discount += line_discount_amt
            total_vat += line_vat

            item_rows.append(InvoiceItem(
                invoice=invoice,
                product=product,
                description=ln.description or getattr(product, "sales_description", "") or "",
                qty=qty,
                unit_price=rate,
                amount=line_amount,
                vat=line_vat,
                discount_num=dpc,
                discount_amount=line_discount_amt,
            ))

        if item_rows:
            InvoiceItem.objects.bulk_create(item_rows)

        total_due = (subtotal - total_discount + total_vat + Decimal(rec.shipping_fee or 0)).quantize(Decimal("0.01"))

        invoice.subtotal = subtotal
        invoice.total_discount = total_discount
        invoice.total_vat = total_vat
        invoice.total_due = total_due

        if apply_audit_fields:
            apply_audit_fields(invoice)

        invoice.save()

        if _post_invoice_to_ledger:
            _post_invoice_to_ledger(invoice)

        RecurringGeneratedInvoice.objects.create(
            recurring=rec,
            invoice=invoice,
            run_date=rec.next_run_date,
        )

        rec.occurrences_generated = (rec.occurrences_generated or 0) + 1
        rec.next_run_date = _compute_next_run(rec.next_run_date, rec.frequency, rec.interval)

        # deactivate if end reached after increment
        if rec.end_date and rec.next_run_date > rec.end_date:
            rec.is_active = False

        if rec.max_occurrences and rec.occurrences_generated >= rec.max_occurrences:
            rec.is_active = False

        rec.save()

        created += 1

    return {
        "run_date": str(run_date),
        "created": created,
        "skipped": skipped,
        "deactivated": deactivated,
    }
