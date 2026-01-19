from decimal import Decimal
from django.db.models import Sum, Value
from django.db.models.functions import Coalesce
from accounts.models import Account,JournalEntry,JournalLine
from sowaf.models import Newsupplier
from expenses.views import _get_or_create_supplier_ap_subaccount

def _supplier_ap_balance_live(supplier_id: int) -> Decimal:
    """
    LIVE Supplier A/P balance from JournalLines.

    For A/P (liability): balance = credits - debits
    because Bills usually CREDIT A/P, and Payments (cheques/expenses) DEBIT A/P.
    """
    sup = Newsupplier.objects.filter(id=supplier_id).first()
    if not sup:
        return Decimal("0.00")

    # Get the supplier subledger account used in posting
    ap_acc = _get_or_create_supplier_ap_subaccount(sup)

    agg = (
        JournalLine.objects
        .filter(account_id=ap_acc.id, supplier_id=supplier_id)
        .aggregate(
            dr=Coalesce(Sum("debit"), Value(Decimal("0.00"))),
            cr=Coalesce(Sum("credit"), Value(Decimal("0.00"))),
        )
    )

    dr = agg["dr"] or Decimal("0.00")
    cr = agg["cr"] or Decimal("0.00")
    return (cr - dr)


def _supplier_ap_balances_bulk(supplier_ids):
    """
    BULK version for Supplier List page (fast).
    Returns dict: {supplier_id: balance}
    """
    supplier_ids = [int(x) for x in supplier_ids if x]
    if not supplier_ids:
        return {}

    # Build supplier_id -> supplier_subledger_account_id
    sups = list(Newsupplier.objects.filter(id__in=supplier_ids))

    acc_map = {}  # supplier_id -> ap_account_id
    for s in sups:
        acc = _get_or_create_supplier_ap_subaccount(s)
        acc_map[s.id] = acc.id

    # Pull all relevant journal lines in one go
    # and sum per supplier (credits - debits)
    lines = (
        JournalLine.objects
        .filter(supplier_id__in=supplier_ids, account_id__in=acc_map.values())
        .values("supplier_id")
        .annotate(
            dr=Coalesce(Sum("debit"), Value(Decimal("0.00"))),
            cr=Coalesce(Sum("credit"), Value(Decimal("0.00"))),
        )
    )

    out = {sid: Decimal("0.00") for sid in supplier_ids}
    for row in lines:
        sid = row["supplier_id"]
        out[sid] = (row["cr"] or Decimal("0.00")) - (row["dr"] or Decimal("0.00"))

    return out
