import random
from decimal import Decimal

from django.db.models import Sum, Q

from .models import Expense, Bill, ChequeBillLine, Cheque


def _dec(x, default="0.00") -> Decimal:
    try:
        return Decimal(str(x if x not in (None, "") else default))
    except Exception:
        return Decimal(default)


def generate_unique_ref_no(company) -> str:
    """
    Return an 8-digit, zero-padded numeric reference unique per company for Expenses.
    """
    for _ in range(20):
        ref = f"{random.randrange(10**8):08d}"
        if not Expense.objects.for_company(company).filter(ref_no=ref).exists():
            return ref
    raise RuntimeError("Could not generate a unique expense reference number.")


def generate_unique_bill_no(company) -> str:
    """
    Return an 8-digit, zero-padded numeric bill number unique per company for Bills.
    """
    for _ in range(20):
        ref = f"{random.randrange(10**8):08d}"
        if not Bill.objects.for_company(company).filter(bill_no=ref).exists():
            return ref
    raise RuntimeError("Could not generate a unique bill number.")


def _bill_balance(bill: Bill, exclude_cheque_id=None) -> Decimal:
    """
    Bill balance = total_amount - sum(applied via cheques).
    Tenant-safe because bill already belongs to one company.
    Optionally exclude one cheque during edit calculations.
    """
    total = _dec(bill.total_amount)

    qs = ChequeBillLine.objects.filter(bill=bill)
    if exclude_cheque_id:
        qs = qs.exclude(cheque_id=exclude_cheque_id)

    applied = qs.aggregate(s=Sum("amount_applied"))["s"] or Decimal("0.00")
    bal = total - _dec(applied)
    return bal if bal > 0 else Decimal("0.00")


def bankish_q():
    """
    IMPORTANT: Use DETAIL TYPE (your COA has 3 layers).
    If your bank accounts have detail_type like 'Bank', 'Cash and Cash Equivalents', etc.
    this will catch them.
    """
    return (
        Q(detail_type__icontains="bank") |
        Q(detail_type__icontains="cash") |
        Q(detail_type__icontains="cash and cash equivalents") |
        Q(detail_type__icontains="cash on hand")
    )


def _save_cheque_bill_allocations(request, cheque: Cheque):
    """
    Reads posted fields: amount_paid_<bill_id>
    Creates ChequeBillLine rows for amounts > 0.
    Replaces existing allocations for this cheque (safe for edits).

    Tenant-safe:
    - only bills from cheque.company
    - only bills for cheque.payee_supplier
    """
    company = getattr(cheque, "company", None)

    # wipe old allocations for this cheque (edit-safe)
    ChequeBillLine.objects.filter(cheque=cheque).delete()

    if not cheque.payee_supplier_id or not company:
        return

    supplier_id = cheque.payee_supplier_id

    # Only this company's bills for this supplier
    bills = Bill.objects.for_company(company).filter(supplier_id=supplier_id)

    # Current applied totals after deleting this cheque's old allocations
    applied_map = dict(
        ChequeBillLine.objects
        .filter(
            bill__company=company,
            bill__supplier_id=supplier_id,
        )
        .values("bill_id")
        .annotate(s=Sum("amount_applied"))
        .values_list("bill_id", "s")
    )

    for b in bills:
        field = f"amount_paid_{b.id}"
        raw = request.POST.get(field)

        if raw in (None, ""):
            continue

        amt = _dec(raw, "0.00")
        if amt <= 0:
            continue

        total = _dec(b.total_amount)
        already_applied = _dec(applied_map.get(b.id), "0.00")
        balance = total - already_applied

        # clamp to outstanding balance
        if amt > balance:
            amt = balance

        if amt <= 0:
            continue

        ChequeBillLine.objects.create(
            cheque=cheque,
            bill=b,
            amount_applied=amt,
        )