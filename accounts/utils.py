# accounts/utils.py
from django.db.models import Q
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
