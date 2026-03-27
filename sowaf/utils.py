from decimal import Decimal

from django.db.models import Sum, Value
from django.db.models.functions import Coalesce

from accounts.models import JournalLine
from sowaf.models import Newsupplier
from expenses.views import _get_or_create_supplier_ap_subaccount


def _supplier_ap_balance_live(supplier_id: int, company=None) -> Decimal:
    """
    LIVE Supplier A/P balance from JournalLines.

    For A/P (liability): balance = credits - debits
    because Bills usually CREDIT A/P, and Payments (cheques/expenses) DEBIT A/P.

    TENANT SAFE:
    - supplier is scoped by company when company is provided
    - journal lines are scoped by entry__company
    """
    supplier_qs = Newsupplier.objects.all()
    if company is not None and hasattr(Newsupplier.objects, "for_company"):
        supplier_qs = Newsupplier.objects.for_company(company)
    elif company is not None and hasattr(Newsupplier, "company_id"):
        supplier_qs = supplier_qs.filter(company=company)

    sup = supplier_qs.filter(id=supplier_id).first()
    if not sup:
        return Decimal("0.00")

    # Get/create the supplier subledger account safely for this company
    try:
        ap_acc = _get_or_create_supplier_ap_subaccount(sup, company=company)
    except TypeError:
        ap_acc = _get_or_create_supplier_ap_subaccount(sup)

    if not ap_acc:
        return Decimal("0.00")

    lines_qs = JournalLine.objects.filter(
        account_id=ap_acc.id,
        supplier_id=supplier_id,
    )

    # CRITICAL: tenant scoping through JournalEntry
    if company is not None:
        lines_qs = lines_qs.filter(entry__company=company)

    agg = lines_qs.aggregate(
        dr=Coalesce(Sum("debit"), Value(Decimal("0.00"))),
        cr=Coalesce(Sum("credit"), Value(Decimal("0.00"))),
    )

    dr = agg["dr"] or Decimal("0.00")
    cr = agg["cr"] or Decimal("0.00")
    return cr - dr


def _supplier_ap_balances_bulk(supplier_ids, company=None):
    """
    BULK version for Supplier List page (fast).
    Returns dict: {supplier_id: balance}

    TENANT SAFE:
    - suppliers are scoped by company
    - journal lines are scoped by entry__company
    """
    supplier_ids = [int(x) for x in supplier_ids if x]
    if not supplier_ids:
        return {}

    supplier_qs = Newsupplier.objects.filter(id__in=supplier_ids)
    if company is not None and hasattr(Newsupplier.objects, "for_company"):
        supplier_qs = Newsupplier.objects.for_company(company).filter(id__in=supplier_ids)
    elif company is not None and hasattr(Newsupplier, "company_id"):
        supplier_qs = supplier_qs.filter(company=company)

    sups = list(supplier_qs)

    if not sups:
        return {}

    # Build supplier_id -> supplier_subledger_account_id
    acc_map = {}
    for s in sups:
        try:
            acc = _get_or_create_supplier_ap_subaccount(s, company=company)
        except TypeError:
            acc = _get_or_create_supplier_ap_subaccount(s)

        if acc:
            acc_map[s.id] = acc.id

    if not acc_map:
        return {sid: Decimal("0.00") for sid in supplier_ids}

    lines_qs = JournalLine.objects.filter(
        supplier_id__in=list(acc_map.keys()),
        account_id__in=list(acc_map.values()),
    )

    # CRITICAL: tenant scoping through JournalEntry
    if company is not None:
        lines_qs = lines_qs.filter(entry__company=company)

    lines = (
        lines_qs
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