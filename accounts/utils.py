# accounts/utils.py
from django.db.models import Q
from decimal import Decimal
from django.utils import timezone
from accounts.models import Account

# Level-3 detail types that represent cash/bank accounts
DEPOSIT_DETAIL_TYPES = [
    "Cash and Cash equivalents",  # exactly as in your COA detail list
    "Bank",                       # for bank accounts
]

def deposit_accounts_qs():
    """
    Accounts allowed in 'Deposit To' dropdown:

    - Any account whose detail_type is one of DEPOSIT_DETAIL_TYPES
    - Any sub-account whose parent has a detail_type in DEPOSIT_DETAIL_TYPES
    """
    return (
        Account.objects
        .filter(
            Q(detail_type__in=DEPOSIT_DETAIL_TYPES) |
            Q(parent__detail_type__in=DEPOSIT_DETAIL_TYPES),
            is_active=True,
        )
        .order_by("account_name", "account_number")
    )

# ==== NEW: Expense accounts helper ====

EXPENSE_ACCOUNT_TYPES = [
    "OPERATING_EXPENSE",
    "INVESTING_EXPENSE",
    "FINANCING_EXPENSE",
    "INCOME_TAX_EXPENSE",
]

def expense_accounts_qs():
    """
    Accounts that can be used as expense categories on the expense form
    (and that we will DR in the General Ledger).

    Includes both level-2 accounts and their sub-accounts.
    """
    return (
        Account.objects
        .filter(
            Q(account_type__in=EXPENSE_ACCOUNT_TYPES) |
            Q(parent__account_type__in=EXPENSE_ACCOUNT_TYPES),
            is_active=True,
        )
        .order_by("account_name", "account_number")
    )

# ==== NEW: Income accounts helper ====

INCOME_ACCOUNT_TYPES = [
    "OPERATING_INCOME",
    "INVESTING_INCOME",
]

def income_accounts_qs():
    """
    Accounts that can be used as income categories on the product/service form
    (and that we will CR in the General Ledger).

    Includes both level-2 income accounts and their sub-accounts.
    """
    return (
        Account.objects
        .filter(
            Q(account_type__in=INCOME_ACCOUNT_TYPES) |
            Q(parent__account_type__in=INCOME_ACCOUNT_TYPES),
            is_active=True,
        )
        .order_by("account_name", "account_number")
    )


VAT_RATE = Decimal("0.18")  # change if needed


def _find_control_account(name_contains: str):
    return Account.objects.filter(
        account_name__icontains=name_contains,
        is_active=True
    ).order_by("id").first()


def _get_or_create_control_account(
    name: str,
    account_type: str,
    detail_type: str | None = None,
):
    acc = Account.objects.filter(account_name__iexact=name, is_active=True).first()
    if acc:
        return acc

    return Account.objects.create(
        account_name=name,
        account_type=account_type,  # must be one of your ACCOUNT_TYPES codes
        detail_type=detail_type,
        is_active=True,
        as_of=timezone.localdate(),
        opening_balance=Decimal("0.00"),
    )


def _get_inventory_asset_account() -> Account:
    # Current Asset
    # detail_type can be anything you use; keep it consistent in your COA
    acc = _find_control_account("Inventory")
    if acc and acc.account_type == "CURRENT_ASSET":
        return acc
    return _get_or_create_control_account(
        name="Inventory Asset",
        account_type="CURRENT_ASSET",
        detail_type="Inventory",
    )


def _get_cogs_account() -> Account:
    # Operating Expense is the cleanest fit in your structure
    acc = _find_control_account("Cost of Sales") or _find_control_account("COGS")
    if acc and acc.account_type in ("OPERATING_EXPENSE", "INVESTING_EXPENSE", "FINANCING_EXPENSE", "INCOME_TAX_EXPENSE"):
        return acc
    return _get_or_create_control_account(
        name="Cost of Sales",
        account_type="OPERATING_EXPENSE",
        detail_type="Cost of Sales",
    )


def _get_vat_payable_account() -> Account:
    # Sales VAT collected (liability)
    acc = _find_control_account("VAT Payable") or _find_control_account("Output VAT") or _find_control_account("VAT")
    if acc and acc.account_type == "CURRENT_LIABILITY":
        return acc
    return _get_or_create_control_account(
        name="VAT Payable",
        account_type="CURRENT_LIABILITY",
        detail_type="VAT",
    )


def _get_vat_receivable_account() -> Account:
    # Purchase VAT / Input VAT (asset)
    acc = _find_control_account("VAT Receivable") or _find_control_account("Input VAT")
    if acc and acc.account_type == "CURRENT_ASSET":
        return acc
    return _get_or_create_control_account(
        name="VAT Receivable",
        account_type="CURRENT_ASSET",
        detail_type="VAT",
    )
