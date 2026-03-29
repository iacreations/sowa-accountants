# accounts/utils.py
from decimal import Decimal

from django.db.models import Q
from django.utils import timezone

from accounts.models import Account


# Level-3 detail types that represent cash/bank accounts
DEPOSIT_DETAIL_TYPES = [
    "Cash and Cash equivalents",
    "Bank",
]

EXPENSE_ACCOUNT_TYPES = [
    "OPERATING_EXPENSE",
    "INVESTING_EXPENSE",
    "FINANCING_EXPENSE",
    "INCOME_TAX_EXPENSE",
]

INCOME_ACCOUNT_TYPES = [
    "OPERATING_INCOME",
    "INVESTING_INCOME",
]

VAT_RATE = Decimal("0.18")


def _company_scoped_qs(company=None):
    """
    Base tenant-safe queryset.
    """
    qs = Account.objects.all()
    if company is not None:
        if hasattr(Account.objects, "for_company"):
            return Account.objects.for_company(company)
        if hasattr(Account, "company_id"):
            return qs.filter(company=company)
    return qs


def deposit_accounts_qs(company=None):
    """
    Accounts allowed in 'Deposit To' dropdown:

    - Any account whose detail_type is one of DEPOSIT_DETAIL_TYPES
    - Any sub-account whose parent has a detail_type in DEPOSIT_DETAIL_TYPES
    """
    return (
        _company_scoped_qs(company)
        .filter(
            Q(detail_type__in=DEPOSIT_DETAIL_TYPES) |
            Q(parent__detail_type__in=DEPOSIT_DETAIL_TYPES) |
            Q(detail_type__icontains="cash") |
            Q(detail_type__icontains="bank") |
            Q(parent__detail_type__icontains="cash") |
            Q(parent__detail_type__icontains="bank"),
            is_active=True,
        )
        .order_by("account_name", "account_number")
    )


def expense_accounts_qs(company=None):
    """
    Accounts that can be used as expense categories.

    Includes both main expense accounts and their subaccounts.
    """
    return (
        _company_scoped_qs(company)
        .filter(
            Q(account_type__in=EXPENSE_ACCOUNT_TYPES) |
            Q(parent__account_type__in=EXPENSE_ACCOUNT_TYPES),
            is_active=True,
        )
        .order_by("account_name", "account_number")
    )


def income_accounts_qs(company=None):
    """
    Accounts that can be used as income categories.

    Includes both main income accounts and their subaccounts.
    """
    return (
        _company_scoped_qs(company)
        .filter(
            Q(account_type__in=INCOME_ACCOUNT_TYPES) |
            Q(parent__account_type__in=INCOME_ACCOUNT_TYPES),
            is_active=True,
        )
        .order_by("account_name", "account_number")
    )


def _find_control_account(name_contains: str, company=None):
    """
    Tenant-safe lookup for a control/system account by partial name.
    """
    return (
        _company_scoped_qs(company)
        .filter(
            account_name__icontains=name_contains,
            is_active=True,
        )
        .order_by("id")
        .first()
    )


def _get_or_create_control_account(
    name: str,
    account_type: str,
    detail_type: str | None = None,
    company=None,
):
    """
    Tenant-safe get/create for a control/system account.
    """
    acc = (
        _company_scoped_qs(company)
        .filter(account_name__iexact=name, is_active=True)
        .first()
    )
    if acc:
        return acc

    create_kwargs = {
        "account_name": name,
        "account_type": account_type,
        "detail_type": detail_type,
        "is_active": True,
        "as_of": timezone.localdate(),
        "opening_balance": Decimal("0.00"),
    }

    if company is not None and hasattr(Account, "company_id"):
        create_kwargs["company"] = company

    return Account.objects.create(**create_kwargs)


def _get_inventory_asset_account(company=None) -> Account:
    """
    Inventory Asset control account.
    """
    acc = _find_control_account("Inventory", company=company)
    if acc and acc.account_type == "CURRENT_ASSET":
        return acc

    return _get_or_create_control_account(
        name="Inventory Asset",
        account_type="CURRENT_ASSET",
        detail_type="Inventory",
        company=company,
    )


def _get_cogs_account(company=None) -> Account:
    """
    Cost of Goods Sold / Cost of Sales account.
    """
    acc = (
        _find_control_account("Cost of Sales", company=company) or
        _find_control_account("COGS", company=company)
    )
    if acc and acc.account_type in EXPENSE_ACCOUNT_TYPES:
        return acc

    return _get_or_create_control_account(
        name="Cost of Sales",
        account_type="OPERATING_EXPENSE",
        detail_type="Cost of Sales",
        company=company,
    )


def _get_vat_payable_account(company=None) -> Account:
    """
    Sales VAT collected (liability).
    """
    acc = (
        _find_control_account("VAT Payable", company=company) or
        _find_control_account("Output VAT", company=company) or
        _find_control_account("VAT", company=company)
    )
    if acc and acc.account_type == "CURRENT_LIABILITY":
        return acc

    return _get_or_create_control_account(
        name="VAT Payable",
        account_type="CURRENT_LIABILITY",
        detail_type="VAT",
        company=company,
    )


def _get_vat_receivable_account(company=None) -> Account:
    """
    Purchase VAT / Input VAT (asset).
    """
    acc = (
        _find_control_account("VAT Receivable", company=company) or
        _find_control_account("Input VAT", company=company)
    )
    if acc and acc.account_type == "CURRENT_ASSET":
        return acc

    return _get_or_create_control_account(
        name="VAT Receivable",
        account_type="CURRENT_ASSET",
        detail_type="VAT",
        company=company,
    )