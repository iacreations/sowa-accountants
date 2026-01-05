import random
from . models import Expense
from decimal import Decimal
from django.db.models import Sum
from django.http import JsonResponse

from django.db.models import Q
from .models import Bill, ChequeBillLine, Cheque
from sowaf.models import Newsupplier
def _dec(x, default="0.00") -> Decimal:
    try:
        return Decimal(str(x if x not in (None, "") else default))
    except Exception:
        return Decimal(default)
def generate_unique_ref_no() -> str:
    """Return an 8-digit, zero-padded, numeric reference that isn't used yet."""
    for _ in range(10):  # a few attempts in case of a rare collision
        ref = f"{random.randrange(10**8):08d}"
        if not Expense.objects.filter(ref_no=ref).exists():
            return ref
    # If we somehow failed 10 times, raise; caller can handle or retry
    raise RuntimeError("Could not generate a unique reference number.")

def generate_unique_bill_no() -> str:
    """Return an 8-digit, zero-padded, numeric reference that isn't used yet."""
    for _ in range(10):  # a few attempts in case of a rare collision
        ref = f"{random.randrange(10**8):08d}"
        if not Expense.objects.filter(ref_no=ref).exists():
            return ref
    # If we somehow failed 10 times, raise; caller can handle or retry
    raise RuntimeError("Could not generate a unique reference number.")

# bills helpers

def _bill_balance(bill: Bill) -> Decimal:
    """
    Bill balance = total_amount - sum(applied via cheques).
    (You don't store balance on the Bill model.)
    """
    total = _dec(bill.total_amount)
    applied = (
        ChequeBillLine.objects
        .filter(bill=bill)
        .aggregate(s=Sum("amount_applied"))["s"]
        or Decimal("0.00")
    )
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
    Creates ChequeBillLine rows for amounts > 0
    Replaces existing allocations for this cheque (safe for edits).
    """
    # wipe old allocations for this cheque (edit-safe)
    ChequeBillLine.objects.filter(cheque=cheque).delete()

    if not cheque.payee_supplier_id:
        return

    supplier_id = cheque.payee_supplier_id

    # compute current balances per bill (excluding this cheque since we deleted its allocations already)
    bills = Bill.objects.filter(supplier_id=supplier_id)

    applied_map = dict(
        ChequeBillLine.objects
        .filter(bill__supplier_id=supplier_id)
        .values("bill_id")
        .annotate(s=Sum("amount_applied"))
        .values_list("bill_id", "s")
    )

    for b in bills:
        field = f"amount_paid_{b.id}"
        raw = request.POST.get(field)
        if raw is None or raw == "":
            continue

        amt = _dec(raw, "0")
        if amt <= 0:
            continue

        total = Decimal(str(b.total_amount or "0"))
        already_applied = Decimal(str(applied_map.get(b.id) or "0"))
        balance = total - already_applied

        # clamp to balance to prevent overpaying
        if amt > balance:
            amt = balance

        if amt <= 0:
            continue

        ChequeBillLine.objects.create(
            cheque=cheque,
            bill=b,
            amount_applied=amt
        )


