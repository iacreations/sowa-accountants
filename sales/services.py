# sales/services.py
import logging
import random
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time

from django.db import transaction
from django.db.models import Sum, Value, DecimalField, F, Q
from django.db.models.functions import Coalesce, Cast
from django.utils import timezone

from accounts.models import Account
from .models import Payment

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


def ensure_default_accounts(company=None):
    """
    Ensure minimal accounts exist. Creates them if missing.
    Adjust account_type codes if your Account model uses different choice keys.

    TENANT SAFE when company is provided.
    """
    defaults = [
        ("Accounts Receivable", "CURRENT_ASSET"),
        ("Sales Income", "OPERATING_INCOME"),
    ]

    created = []
    for name, acct_type in defaults:
        qs = Account.objects.all()
        if company is not None and hasattr(Account.objects, "for_company"):
            qs = Account.objects.for_company(company)
        elif company is not None and hasattr(Account, "company_id"):
            qs = qs.filter(company=company)

        acct = qs.filter(account_name=name).first()
        if not acct:
            create_kwargs = {
                "account_name": name,
                "account_type": acct_type,
                "is_active": True,
                "opening_balance": Decimal("0.00"),
                "as_of": timezone.localdate(),
            }
            if company is not None and hasattr(Account, "company_id"):
                create_kwargs["company"] = company

            acct = Account.objects.create(**create_kwargs)
            created.append(acct)
            logger.info("Created default account: %s", acct.account_name)
            print("Created default account:", acct.account_name)

    return created


def get_ar_account(company=None):
    """
    Locate the control Accounts Receivable account.
    Adjust logic if your naming/type codes differ.

    TENANT SAFE when company is provided.
    """
    qs = Account.objects.all()
    if company is not None and hasattr(Account.objects, "for_company"):
        qs = Account.objects.for_company(company)
    elif company is not None and hasattr(Account, "company_id"):
        qs = qs.filter(company=company)

    ar = (
        qs.filter(detail_type__iexact="Accounts Receivable (A/R)", is_active=True).order_by("id").first()
        or qs.filter(account_name__iexact="Accounts Receivable", is_active=True).order_by("id").first()
        or qs.filter(account_name__icontains="receivable", is_active=True).order_by("id").first()
    )
    return ar


def generate_unique_ref_no(company=None) -> str:
    """
    Return an 8-digit, zero-padded, numeric reference that isn't used yet.
    TENANT SAFE when company is provided.
    """
    for _ in range(10):  # a few attempts in case of a rare collision
        ref = f"{random.randrange(10**8):08d}"

        qs = Payment.objects.all()
        if company is not None and hasattr(Payment.objects, "for_company"):
            qs = Payment.objects.for_company(company)
        elif company is not None and hasattr(Payment, "company_id"):
            qs = qs.filter(company=company)

        if not qs.filter(reference_no=ref).exists():
            return ref

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

    TENANT SAFE using payment.company.
    """
    company = getattr(payment, "company", None)

    qs = payment.applied_invoices.select_related("invoice").order_by("id")

    lines = []
    applied_total = Decimal("0.00")
    remaining_total_this_payment = Decimal("0.00")
    outstanding_total_now = Decimal("0.00")

    for pi in qs:
        inv = pi.invoice
        total_due = Decimal(str(getattr(inv, "total_due", 0) or 0))
        amount_applied = Decimal(str(getattr(pi, "amount_paid", 0) or 0))

        remaining_this_payment = Decimal("0.00")

        total_paid_qs = inv.payments_applied.all()
        if company is not None and hasattr(inv, "company_id"):
            total_paid_qs = total_paid_qs.filter(invoice__company=company)

        total_paid_now = total_paid_qs.aggregate(
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
def _get_sales_income_account(company=None):
    """
    Try to find a 'Sales Income' account; fallback to the first income account.
    TENANT SAFE when company is provided.
    """
    qs = Account.objects.all()
    if company is not None and hasattr(Account.objects, "for_company"):
        qs = Account.objects.for_company(company)
    elif company is not None and hasattr(Account, "company_id"):
        qs = qs.filter(company=company)

    acc = (
        qs.filter(account_name__iexact="Sales Income", is_active=True).first()
        or qs.filter(account_name__icontains="Sales", is_active=True).first()
        or qs.filter(account_name__icontains="Revenue", is_active=True).first()
        or qs.filter(
            Q(account_type="OPERATING_INCOME") |
            Q(account_type="INVESTING_INCOME"),
            is_active=True
        ).first()
    )
    return acc


def _coerce_decimal(x, default="0"):
    try:
        return Decimal(str(x or default))
    except Exception:
        return Decimal(default)