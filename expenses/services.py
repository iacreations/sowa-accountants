# utils for GL posting
from decimal import Decimal, ROUND_HALF_UP
from django.db import transaction
from django.utils import timezone

from accounts.models import Account, JournalEntry, JournalLine
from expenses.models import Bill

UNCATEGORIZED_EXP_NAME = "Uncategorized Expense"

def _q(v) -> Decimal:
    """Quantize to 2 dp (accounting style)."""
    return (v or Decimal("0.00")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _is_pos(v: Decimal) -> bool:
    try:
        return Decimal(v) > 0
    except Exception:
        return False

def _get_uncategorized_expense_account():
    acc = Account.objects.filter(account_name=UNCATEGORIZED_EXP_NAME).first()
    if acc:
        return acc
    # Create a simple EXPENSE account as a fallback
    return Account.objects.create(
        account_name=UNCATEGORIZED_EXP_NAME,
        account_type="EXPENSE",
        detail_type="Other Expenses",
        description="Auto-created fallback for item lines without an expense account."
    )

# ---------- Expenses (cash) ----------

@transaction.atomic
def post_expense_to_gl(expense):
    """
    Create a JournalEntry for the given Expense.
    DR: each category/item line to its expense/COGS account
    CR: payment account with total amount
    """
    # Pull related lines efficiently (use your actual related_name values)
    expense = (
        expense.__class__.objects
        .select_related("payment_account", "payee_supplier")
        .prefetch_related("cat_lines__category", "item_lines__product")
        .get(pk=expense.pk)
    )

    ref = expense.ref_no or f"EXP-{expense.pk}"
    who = (
        expense.payee_supplier.company_name
        if getattr(expense, "payee_supplier", None)
        else (expense.payee_name or "Payee")
    )
    desc = f"Expense from {who} with Ref- {ref}"

    total_credit = Decimal("0.00")
    lines_to_create = []

    # Debits: Category lines
    for cl in getattr(expense, "cat_lines", []).all():
        amt = _q(cl.amount)
        if not _is_pos(amt) or not cl.category:
            continue
        lines_to_create.append(
            JournalLine(
                account=cl.category,
                debit=amt, credit=Decimal("0.00")
            )
        )
        total_credit += amt

    # Debits: Item lines → product.expense_account / COGS / fallback
    for il in getattr(expense, "item_lines", []).all():
        amt = _q(il.amount)
        if not _is_pos(amt):
            continue
        prod = il.product
        exp_acc = getattr(prod, "expense_account", None) or getattr(prod, "cogs_account", None)
        if exp_acc is None:
            exp_acc = _get_uncategorized_expense_account()
        lines_to_create.append(
            JournalLine(
                account=exp_acc,
                debit=amt, credit=Decimal("0.00")
            )
        )
        total_credit += amt

    # Bail out gracefully if nothing to post
    if total_credit <= 0:
        return None

    # Credit: payment account
    pay_acc = getattr(expense, "payment_account", None)
    if pay_acc:
        lines_to_create.append(
            JournalLine(
                account=pay_acc,
                debit=Decimal("0.00"), credit=_q(total_credit)
            )
        )

    # Create JE + lines
    je = JournalEntry.objects.create(
        date=expense.payment_date or timezone.localdate(),
        description=desc,
    )
    for jl in lines_to_create:
        jl.entry = je
    JournalLine.objects.bulk_create(lines_to_create)

    return je


# ---------- Bills (A/P) ----------
def _q(v: Decimal) -> Decimal:
    return (v or Decimal("0.00")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _find_or_create_ap() -> Account:
    # 1) Try by type
    ap = Account.objects.filter(account_type__in=[
        "AP", "A/P", "ACCOUNTS_PAYABLE", "Accounts Payable (A/P)", "Accounts Payable"
    ]).first()
    if ap:
        return ap
    # 2) Try by name
    ap = Account.objects.filter(account_name__iexact="Accounts Payable").first() \
         or Account.objects.filter(account_name__icontains="payable").first()
    if ap:
        return ap
    # 3) Create minimal A/P (use only fields that exist in your model)
    return Account.objects.create(
        account_name="Accounts Payable",
        account_type="ACCOUNTS_PAYABLE",
    )

@transaction.atomic
def post_bill_to_ledger(bill: Bill):
    """
    Bills: DR expense/COS lines, CR Accounts Payable.
    """
    # Re-fetch with related for fewer queries
    bill = (
        Bill.objects
        .select_related("supplier")
        .prefetch_related("category_lines__category", "item_lines__product")
        .get(pk=bill.pk)
    )

    ap = _find_or_create_ap()

    memo = f"Bill {bill.bill_no or bill.id}"
    if bill.supplier:
        memo += f" - {bill.supplier.company_name}"
    elif getattr(bill, "supplier_name", ""):
        memo += f" - {bill.supplier_name}"

    total_debits = Decimal("0.00")
    jl = []

    # DR category expense accounts
    for line in bill.category_lines.all():
        amt = _q(line.amount)
        if amt <= 0 or not line.category:
            continue
        jl.append(JournalLine(account=line.category, debit=amt, credit=Decimal("0.00")))
        total_debits += amt

    # DR item lines → product.expense_account (or a generic expense if missing)
    for line in bill.item_lines.all():
        amt = _q(line.amount)
        if amt <= 0:
            continue
        target = getattr(line.product, "expense_account", None)
        if target is None:
            # Find a generic expense/COS account to use
            target = Account.objects.filter(
                account_type__in=["COST_OF_SALES", "Expense", "EXPENSE", "Other Expenses", "OTHER_EXPENSE"]
            ).order_by("id").first()
        if target is None:
            # Last resort: small fallback expense account
            target = Account.objects.get_or_create(
                account_name="Purchases", account_type="EXPENSE"
            )[0]

        jl.append(JournalLine(account=target, debit=amt, credit=Decimal("0.00")))
        total_debits += amt

    if total_debits <= 0:
        # Nothing to post; skip creating a JE
        return None

    je = JournalEntry.objects.create(
        date=bill.bill_date or timezone.localdate(),
        description=memo
    )

    # CR A/P
    jl.append(JournalLine(account=ap, debit=Decimal("0.00"), credit=_q(total_debits)))

    for line in jl:
        line.entry = je
    JournalLine.objects.bulk_create(jl)

    return je
# working on the cheque

def post_cheque_to_ledger(cheque):
    """
    DR expense accounts from lines
    CR the bank/cash account for the total
    """
    je = JournalEntry.objects.create(
        date=cheque.payment_date,
        description=f"Cheque {cheque.cheque_no} - {cheque.payee_supplier.company_name if cheque.payee_supplier else cheque.payee_name or 'Payee'}"
    )

    total = Decimal("0.00")

    # Category lines → expense accounts
    for cl in cheque.category_lines.select_related("category"):
        if cl.amount and cl.amount > 0:
            JournalLine.objects.create(entry=je, account=cl.category, debit=cl.amount, credit=Decimal("0.00"))
            total += cl.amount

    # Item lines → product expense/COGS (fallback to an expense/purchases)
    for il in cheque.item_lines.select_related("product"):
        if not il.amount or il.amount <= 0:
            continue
        target = getattr(il.product, "expense_account", None) or getattr(il.product, "cogs_account", None)
        if target is None:
            target = Account.objects.filter(account_type__in=["EXPENSE", "OTHER_EXPENSE", "COST_OF_SALES", "Expense", "Other Expenses", "Cost of Sales"]).first()
        if target is None:
            target = Account.objects.create(account_name="Purchases", account_type="EXPENSE")
        JournalLine.objects.create(entry=je, account=target, debit=il.amount, credit=Decimal("0.00"))
        total += il.amount

    if total > 0:
        JournalLine.objects.create(entry=je, account=cheque.bank_account, debit=Decimal("0.00"), credit=total)

    return je