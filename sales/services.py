# sales/services.py
import logging
from decimal import Decimal, InvalidOperation
from django.db import transaction
from django.db.models import Sum, Value, DecimalField, F
from datetime import datetime, date
from django.db.models.functions import Coalesce, Cast
import random
from django.db.models import Q
from accounts.models import Account
from .models import Payment
from datetime import datetime, date, time
from django.utils import timezone
logger = logging.getLogger(__name__)

def as_aware_datetime(value):
    """
    Convert:
      - date -> aware datetime at 00:00
      - naive datetime -> aware datetime
      - aware datetime -> unchanged
      - None -> None
    """
    if not value:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time.min)

    if timezone.is_naive(value):
        return timezone.make_aware(value, timezone.get_current_timezone())
    return value

def ensure_default_accounts():
    """
    Ensure minimal accounts exist. Creates them if missing.
    Adjust account_type codes if your Account model uses different choice keys.
    """
    defaults = [
        ("Accounts Receivable", "AR"),
        ("Sales Income", "INCOME"),
    ]

    created = []
    for name, acct_type in defaults:
        acct, was_created = Account.objects.get_or_create(
            account_name=name,
            defaults={"account_type": acct_type},
        )
        if was_created:
            created.append(acct)
            logger.info("Created default account: %s", acct.account_name)
            print("Created default account:", acct.account_name)
    return created



def get_ar_account():
    """
    Locate the control Accounts Receivable account.
    Adjust logic if your naming/type codes differ.
    """
    ar = Account.objects.filter(account_type__iexact="AR").order_by("id").first()
    if not ar:
        ar = Account.objects.filter(account_name__iexact="Accounts Receivable").order_by("id").first()
    return ar



def generate_unique_ref_no() -> str:
    """Return an 8-digit, zero-padded, numeric reference that isn't used yet."""
    for _ in range(10):  # a few attempts in case of a rare collision
        ref = f"{random.randrange(10**8):08d}"
        if not Payment.objects.filter(reference_no=ref).exists():
            return ref
    # If we somehow failed 10 times, raise; caller can handle or retry
    raise RuntimeError("Could not generate a unique reference number.")


# date prefixes

def parse_date_flexible(s):
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

# util for invoice status

def _as_date(d):
    """
    Normalize a value that could be:
    - None
    - datetime.date
    - datetime.datetime
    into a datetime.date (or None)
    """
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return None


def status_for_invoice(inv, total_due: Decimal, total_paid: Decimal, balance: Decimal) -> str:
    # timezone-safe today
    today = timezone.localdate()

    # normalize due_date (DateTimeField can return datetime)
    due = _as_date(getattr(inv, "due_date", None))

    overdue_days = None
    if due and balance > 0 and today > due:
        overdue_days = (today - due).days

    deposited = False
    if total_due > 0 and balance == 0:
        aps = inv.payments_applied.select_related("payment__deposit_to").all()
        if aps:
            def is_bankish(acc):
                if not acc:
                    return False
                at = (acc.account_type or "").lower()
                dt = (acc.detail_type or "").lower()
                return (
                    at in ("bank", "cash and cash equivalents", "cash_equiv", "cash & cash equivalents")
                    or "bank" in dt
                )
            deposited = all(is_bankish(pi.payment.deposit_to) for pi in aps if pi.payment)

    if total_due == 0:
        return "No amount"
    if balance == 0:
        return "Deposited" if deposited else "Paid"

    if overdue_days:
        return (
            f"Overdue by {overdue_days} days — "
            f"{'Partially paid. ' if total_paid > 0 else ''}{balance:,.0f} is remaining"
        )

    if due and due == today and balance > 0:
        return f"Due today — {balance:,.0f} is remaining"

    return (
        f"Partially paid. {balance:,.0f} is remaining"
        if total_paid > 0
        else f"{balance:,.0f} is remaining"
    )


def _payment_prefill_rows(payment):
    """
    Returns a dict the template expects:
      {
        "payment": <Payment>,
        "lines": [ { invoice, total_due, amount_applied, remaining_this_payment, outstanding_now }, ... ],
        "applied_total": Decimal,
        "remaining_total_this_payment": Decimal,
        "outstanding_total_now": Decimal,
      }
    """
    qs = payment.applied_invoices.select_related("invoice").order_by("id")

    lines = []
    applied_total = Decimal("0.00")
    remaining_total_this_payment = Decimal("0.00")
    outstanding_total_now = Decimal("0.00")

    for pi in qs:
        inv = pi.invoice
        total_due = Decimal(inv.total_due or 0)
        amount_applied = Decimal(pi.amount_paid or 0)

        remaining_this_payment = Decimal("0.00")

        total_paid_now = inv.payments_applied.aggregate(
            s=Coalesce(Sum("amount_paid"), Value(Decimal("0.00")))
        )["s"] or Decimal("0.00")

        outstanding_now_row = total_due - total_paid_now
        if outstanding_now_row < 0:
            outstanding_now_row = Decimal("0.00")

        lines.append({
            "invoice": inv,
            "total_due": total_due,
            "amount_applied": amount_applied,
            "remaining_this_payment": remaining_this_payment,
            "outstanding_now": outstanding_now_row,
        })

        applied_total += amount_applied
        remaining_total_this_payment += remaining_this_payment
        outstanding_total_now += outstanding_now_row

    return {
        "payment": payment,
        "lines": lines,
        "applied_total": applied_total,
        "remaining_total_this_payment": remaining_total_this_payment,
        "outstanding_total_now": outstanding_total_now,
    }


# working on the sales receipt

def _get_sales_income_account():
    """
    Try to find a 'Sales Income' account; fallback to the first INCOME account.
    """
    acc = Account.objects.filter(account_name__iexact="Sales Income").first()
    if acc:
        return acc
    return Account.objects.filter(Q(account_type__iexact="INCOME") | Q(account_type__icontains="income")).first()



def _coerce_decimal(x, default="0"):
    try:
        return Decimal(x or default)
    except Exception:
        return Decimal(default)
