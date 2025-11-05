# sales/services.py
import logging
from decimal import Decimal, InvalidOperation
from django.db import transaction
from django.db.models import Sum, Value, DecimalField, F
from datetime import datetime, date
from django.db.models.functions import Coalesce, Cast
import random
from django.db.models import Q
from accounts.models import Account, JournalEntry, JournalLine
from .models import Payment, PaymentInvoice, SalesReceipt, SalesReceiptLine  # your sales models

logger = logging.getLogger(__name__)


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


@transaction.atomic
def post_invoice(invoice, create_defaults=True):
    """
    Post an invoice to the journal (DR Accounts Receivable / CR Sales Income).
    Returns the created JournalEntry (or None if skipped).
    """
    if create_defaults:
        ensure_default_accounts()

    # get required accounts
    try:
        ar = Account.objects.get(account_name="Accounts Receivable")
        sales_income = Account.objects.get(account_name="Sales Income")
    except Account.DoesNotExist as e:
        logger.exception("Required account missing: %s", e)
        raise

    # convert total_due safely
    try:
        amount = Decimal(invoice.total_due)
    except (InvalidOperation, TypeError, ValueError):
        amount = Decimal(str(invoice.total_due or "0"))

    if amount <= 0:
        msg = f"Invoice {invoice.id} has non-positive total_due ({amount}); skipping journal post."
        logger.warning(msg)
        print(msg)
        return None

    # create journal entry
    je = JournalEntry.objects.create(
        description=f"Invoice {invoice.id}",
        invoice=invoice,
    )

    # add debit/credit lines
    JournalLine.objects.create(entry=je, account=ar, debit=amount, credit=0)
    JournalLine.objects.create(entry=je, account=sales_income, debit=0, credit=amount)

    logger.info("Posted JE %s for invoice %s", je.id, invoice.id)
    print(f"Posted JE {je.id} for invoice {invoice.id}")

    return je


def get_ar_account():
    """
    Locate the control Accounts Receivable account.
    Adjust logic if your naming/type codes differ.
    """
    ar = Account.objects.filter(account_type__iexact="AR").order_by("id").first()
    if not ar:
        ar = Account.objects.filter(account_name__iexact="Accounts Receivable").order_by("id").first()
    return ar


@transaction.atomic
def post_payment(payment: Payment):
    """
    Post a customer payment to the journal:
        DR deposit_to (Bank / Cash & Cash Equivalents)
        CR Accounts Receivable

    Links the JournalEntry to the first allocated invoice (optional).
    Returns the created JournalEntry (or None if nothing applied).
    """
    # total applied to invoices
    total_applied = (
        PaymentInvoice.objects
        .filter(payment=payment)
        .aggregate(total=Sum("amount_paid"))
        .get("total") or Decimal("0")
    )
    if total_applied <= 0:
        return None  # nothing to post

    ar = get_ar_account()
    if not ar:
        raise ValueError("No Accounts Receivable account found in Chart of Accounts.")

    # link to an invoice if there is at least one allocation (first is fine for reporting)
    first_alloc = (
        PaymentInvoice.objects.filter(payment=payment).order_by("id").first()
    )

    je = JournalEntry.objects.create(
        date=payment.payment_date,
        description=f"Payment {payment.id} from {payment.customer.customer_name} "
                    f"({payment.payment_method}) Ref:{payment.reference_no or ''}".strip(),
        invoice=first_alloc.invoice if first_alloc else None,
    )

    # DR deposit account, CR A/R
    JournalLine.objects.create(
        entry=je, account=payment.deposit_to, debit=total_applied, credit=Decimal("0.00")
    )
    JournalLine.objects.create(
        entry=je, account=ar, debit=Decimal("0.00"), credit=total_applied
    )

    logger.info("Posted payment JE %s for payment %s", je.id, payment.id)
    print(f"Posted payment JE {je.id} for payment {payment.id}")

    return je

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

def status_for_invoice(inv, total_due: Decimal, total_paid: Decimal, balance: Decimal) -> str:
    today = date.today()
    overdue_days = None
    if inv.due_date and balance > 0 and today > inv.due_date:
        overdue_days = (today - inv.due_date).days

    deposited = False
    if total_due > 0 and balance == 0:
        aps = inv.payments_applied.select_related("payment__deposit_to").all()
        if aps:
            def is_bankish(acc):
                if not acc: return False
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
        return f"Overdue by {overdue_days} days — {'Partially paid. ' if total_paid > 0 else ''}{balance:,.0f} is remaining"

    if inv.due_date and inv.due_date == today and balance > 0:
        return f"Due today — {balance:,.0f} is remaining"

    return f"Partially paid. {balance:,.0f} is remaining" if total_paid > 0 else f"{balance:,.0f} is remaining"

# payments
# sales/views.py (or wherever your helper lives)
from decimal import Decimal
from django.db.models import Sum, Value
from django.db.models.functions import Coalesce

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

        # remaining within THIS payment (for a single row it's amount_applied - amount_applied = 0)
        remaining_this_payment = Decimal("0.00")

        # outstanding now = total_due - all payments applied to that invoice (including this one)
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
        "applied_total": applied_total,  # never None
        "remaining_total_this_payment": remaining_total_this_payment,
        "outstanding_total_now": outstanding_total_now,
    }


def _delete_existing_payment_journal_entries(payment: Payment):
    """
    If you journaled this payment previously (post_payment),
    remove existing entries so we can re-post cleanly.
    We identify by description prefix 'Payment {id}'.
    If you later add a ForeignKey from JournalEntry->Payment, switch to that.
    """
    JournalEntry.objects.filter(description__startswith=f"Payment {payment.id}").delete()

# working on the sales receipt

def _get_sales_income_account():
    """
    Try to find a 'Sales Income' account; fallback to the first INCOME account.
    """
    acc = Account.objects.filter(account_name__iexact="Sales Income").first()
    if acc:
        return acc
    return Account.objects.filter(Q(account_type__iexact="INCOME") | Q(account_type__icontains="income")).first()


@transaction.atomic
def post_sales_receipt(receipt: SalesReceipt):
    """
    DR deposit_to (Bank/Cash & Cash Equivalents)
    CR Sales Income
    amount = receipt.total_amount
    """
    amount = Decimal(receipt.total_amount or 0)
    if amount <= 0:
        return None

    income = _get_sales_income_account()
    if not income:
        raise ValueError("No Sales Income account found (name 'Sales Income' or account_type='INCOME').")

    # Create an unlinked journal entry; (your JournalEntry has `invoice` FK only)
    je = JournalEntry.objects.create(
        date=receipt.receipt_date,
        description=f"Sales Receipt {receipt.id} - {receipt.customer.customer_name}",
        invoice=None,
    )

    # DR Deposit account, CR Income
    JournalLine.objects.create(entry=je, account=receipt.deposit_to, debit=amount, credit=Decimal("0.00"))
    JournalLine.objects.create(entry=je, account=income,             debit=Decimal("0.00"), credit=amount)
    return je


def delete_sales_receipt_journal(receipt: SalesReceipt):
    """
    Remove previously posted JEs for this receipt (matched by description prefix).
    (Since JournalEntry doesn't have receipt FK, we match by description text.)
    """
    JournalEntry.objects.filter(description__startswith=f"Sales Receipt {receipt.id}").delete()

def _coerce_decimal(x, default="0"):
    try:
        return Decimal(x or default)
    except Exception:
        return Decimal(default)