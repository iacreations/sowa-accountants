# chart_of_accounts/views.py
from django.shortcuts import render, redirect,get_object_or_404
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import re
from django.db.models import Sum, Value, DecimalField
from decimal import Decimal
from django.utils.timezone import make_naive
from django.db.models.functions import Coalesce
from .models import Account,ColumnPreference
from .models import JournalEntry, JournalLine
from django.utils import timezone
from decimal import Decimal
from datetime import timedelta
from django.core.exceptions import FieldDoesNotExist
from django.db.models import Sum, F, Value
from django.db.models.functions import Coalesce
from django.utils.dateparse import parse_date

# my views



DEFAULT_ACCOUNTS_COL_PREFS = {
    "account_name": True,
    "opening_balance": True,
    "as_of": True,
    "account_number": True,
    "account_type": True,
    "detail_type": True,
    "description": True,
    "actions": True,  # keep actions togglable too
}

def accounts(request):
    status = request.GET.get("status", "active")  # default is active

    if status == "inactive":
        coas = Account.objects.filter(is_active=False)
    elif status == "all":
        coas = Account.objects.all().order_by('account_type', 'account_name')
    else:
        coas = Account.objects.filter(is_active=True)

    # counts for badges
    active_count = Account.objects.filter(is_active=True).count()
    inactive_count = Account.objects.filter(is_active=False).count()
    all_count = Account.objects.count()

    # Column preferences:
    # - if logged in: per-user prefs (created on first visit)
    # - if anonymous: just use defaults (don't touch DB)
    if getattr(request.user, "is_authenticated", False):
        prefs, _ = ColumnPreference.objects.get_or_create(
            user=request.user,
            table_name="accounts",
            defaults={"preferences": DEFAULT_ACCOUNTS_COL_PREFS},
        )
        merged_prefs = {**DEFAULT_ACCOUNTS_COL_PREFS, **(prefs.preferences or {})}
    else:
        merged_prefs = DEFAULT_ACCOUNTS_COL_PREFS

    return render(request, 'accounts.html', {
        'coas': coas,
        'status': status,
        'column_prefs': merged_prefs,
        "active_count": active_count,
        "inactive_count": inactive_count,
        "all_count": all_count,
    })

# ajax to fetch the data

@csrf_exempt
def save_column_prefs(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "detail": "POST required"}, status=400)

    try:
        data = json.loads(request.body or "{}")
        preferences = data.get("preferences", {})
    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "detail": "Bad JSON"}, status=400)

    prefs, _ = ColumnPreference.objects.get_or_create(
        user=request.user,
        table_name="accounts",
    )
    # also ensure unknown keys don’t sneak in (optional)
    cleaned = {k: bool(preferences.get(k, True)) for k in DEFAULT_ACCOUNTS_COL_PREFS.keys()}
    prefs.preferences = cleaned
    prefs.save()
    return JsonResponse({"status": "ok"})
# add account view
def add_account(request):

    if request.method == "POST":
        # Get values from POST
        account_name = request.POST.get("account_name")
        account_number = request.POST.get("account_number")
        account_type = request.POST.get("account_type")
        detail_type = request.POST.get("detail_type")
        is_subaccount = request.POST.get("is_subaccount") == "on"  # checkbox
          # optional parent account
        opening_balance = request.POST.get("opening_balance") or 0
        as_of = request.POST.get("as_of") or timezone.now().date()
        description = request.POST.get("description")

        # Handle parent account (if subaccount checked)
        parent_id = request.POST.get("parent")
        parent = None
        if is_subaccount and parent_id:
            try:
                parent = Account.objects.get(id=parent_id)
            except Account.DoesNotExist:
                parent = None

        # Create the account
        new_account = Account(
            account_name=account_name,
            account_number=account_number,
            account_type=account_type,
            detail_type=detail_type,
            is_subaccount=is_subaccount,
            parent=parent,
            opening_balance=opening_balance,
            as_of=as_of,
            description=description
        )
        new_account.save()
        # adding button save actions
        save_action = request.POST.get('save_action')
        if save_action == 'save&new':
            return redirect('add-account')
        elif save_action == 'save&close':
            return redirect('accounts:accounts')
        return redirect("accounts:accounts")  # default
    parents = Account.objects.all()
    return render(request, "coa_form.html", {"parents": parents})

def deactivate_account(request, pk):
    coa = get_object_or_404(Account, pk=pk)
    coa.is_active = False
    coa.save()
    return redirect('accounts:accounts')  # your list view

def activate_account(request, pk):
    coa = get_object_or_404(Account, pk=pk)
    coa.is_active = True
    coa.save()
    return redirect('accounts:accounts')

# working on the COA calcs

def journal_list(request):
    entries = (
        JournalEntry.objects
        .select_related("invoice")
        .prefetch_related("lines__account")
        .order_by("date", "id")
    )

    dec0 = Value(Decimal("0.00"), output_field=DecimalField(max_digits=18, decimal_places=2))

    totals = JournalLine.objects.aggregate(
        total_debit=Coalesce(Sum("debit"), dec0),
        total_credit=Coalesce(Sum("credit"), dec0),
    )

    return render(
        request,
        "journal_entries.html",
        {
            "entries": entries,
            "grand_debit": totals["total_debit"],
            "grand_credit": totals["total_credit"],
        },
    )
# Generating reports

# trial balance
def _period(request):
    # ?from=2025-01-01&to=2025-12-31
    dfrom = parse_date(request.GET.get("from") or "")
    dto   = parse_date(request.GET.get("to") or "")
    return dfrom, dto


def trial_balance(request):
    dfrom = parse_date(request.GET.get("from", "") or "")
    dto   = parse_date(request.GET.get("to", "") or "")

    # NOTE: date lives on the parent entry
    lines = JournalLine.objects.select_related("entry", "account")

    # If your JournalEntry model uses a different field name than `date`,
    # change `entry__date` to the correct one (e.g. `entry__posting_date`).
    if dfrom:
        lines = lines.filter(entry__date__gte=dfrom)
    if dto:
        lines = lines.filter(entry__date__lte=dto)

    agg = (
        lines.values("account_id", "account__account_name")
             .annotate(
                 debit = Coalesce(Sum("debit"),  Value(Decimal("0.00"))),
                 credit= Coalesce(Sum("credit"), Value(Decimal("0.00"))),
             )
             .order_by("account__account_name")
    )

    rows = []
    total_debit = total_credit = Decimal("0.00")
    for r in agg:
        d = r["debit"] or Decimal("0")
        c = r["credit"] or Decimal("0")
        total_debit  += d
        total_credit += c
        rows.append({
            "account": r["account__account_name"] or "—",
            "debit": d,
            "credit": c,
        })

    return render(request, "trial_balance.html", {
        "rows": rows,
        "total_debit": total_debit,
        "total_credit": total_credit,
        "dfrom": dfrom,
        "dto": dto,
    })
# working on the profits and losses



INCOME_TYPES  = {"income", "other income"}
EXPENSE_TYPES = {"expense", "other expense", "cost of goods sold"}

def _apply_entry_date_range(qs, dfrom, dto):
    """
    Apply date range to JournalLine queryset by discovering the
    correct date field on the related JournalEntry (e.g. 'entry_date' or 'date').
    """
    # related model for the FK 'entry'
    EntryModel = qs.model._meta.get_field("entry").remote_field.model
    # choose the date field name
    date_field_name = "entry_date"
    try:
        EntryModel._meta.get_field("entry_date")
    except FieldDoesNotExist:
        date_field_name = "date"

    # build lookups like entry__entry_date__gte or entry__date__gte
    if dfrom:
        qs = qs.filter(**{f"entry__{date_field_name}__gte": dfrom})
    if dto:
        qs = qs.filter(**{f"entry__{date_field_name}__lte": dto})
    return qs


def report_pnl(request):
    dfrom, dto = _period(request)  # your helper used in TB

    lines = JournalLine.objects.select_related("account", "entry")
    lines = _apply_entry_date_range(lines, dfrom, dto)

    agg = (
        lines
        .values("account_id", "account__account_name", "account__account_type")
        .annotate(
            deb=Coalesce(Sum("debit"),  Value(Decimal("0.00"))),
            cre=Coalesce(Sum("credit"), Value(Decimal("0.00"))),
        )
        .order_by("account__account_name")
    )

    buckets = {"income": [], "cogs": [], "expense": []}
    totals  = {"income": Decimal("0"), "cogs": Decimal("0"), "expense": Decimal("0")}

    for a in agg:
        t = (a["account__account_type"] or "").lower()
        rev_like = a["cre"] - a["deb"]     # revenue positive
        exp_like = a["deb"] - a["cre"]     # costs positive

        if t in INCOME_TYPES:
            buckets["income"].append((a["account__account_name"], rev_like))
            totals["income"] += rev_like
        elif t == "cost of goods sold":
            buckets["cogs"].append((a["account__account_name"], exp_like))
            totals["cogs"] += exp_like
        elif t in EXPENSE_TYPES:
            buckets["expense"].append((a["account__account_name"], exp_like))
            totals["expense"] += exp_like

    gross_profit = totals["income"] - totals["cogs"]
    net_profit   = gross_profit - totals["expense"]

    return render(request, "pnl.html", {
        "buckets": buckets,
        "totals": totals,
        "gross_profit": gross_profit,
        "net_profit": net_profit,
        "dfrom": dfrom, "dto": dto,
    })
# working on the balance sheet
INCOME_TYPES  = {"income", "other income"}
EXPENSE_TYPES = {"expense", "other expense", "cost of goods sold"}

ASSET_TYPES   = {
    "bank","cash and cash equivalents","current asset","fixed asset","other asset",
    "accounts receivable","inventory","prepaid expense"
}
LIAB_TYPES    = {"accounts payable","current liability","long term liability","other liability"}
EQUITY_TYPES  = {"equity"}


def _period(request):
    dfrom = request.GET.get("from") or None
    dto   = request.GET.get("to") or None
    from datetime import datetime
    fmt = "%Y-%m-%d"
    try: dfrom = datetime.strptime(dfrom, fmt).date() if dfrom else None
    except: dfrom = None
    try: dto   = datetime.strptime(dto, fmt).date() if dto else None
    except: dto = None
    return dfrom, dto


def _apply_asof(qs, asof):
    if not asof:
        return qs
    EntryModel = qs.model._meta.get_field("entry").remote_field.model
    date_field = "entry_date"
    try:
        EntryModel._meta.get_field("entry_date")
    except FieldDoesNotExist:
        date_field = "date"
    return qs.filter(**{f"entry__{date_field}__lte": asof})


def _iregex_from_types(type_set):
    return "|".join(re.escape(t) for t in type_set)


def _net_by_types(lines, type_set, positive_is_debit=True):
    """
    Returns (rows, total) for the provided account-type bucket.
    - For assets: positive_is_debit=True  -> amount = debit - credit
    - For liab/equity: positive_is_debit=False -> amount = credit - debit
    """
    pattern = _iregex_from_types(type_set)
    agg = (
        lines
        .filter(account__account_type__iregex=pattern)
        .values("account__account_name", "account__account_type")
        .annotate(
            deb=Coalesce(Sum("debit"),  Value(Decimal("0"))),
            cre=Coalesce(Sum("credit"), Value(Decimal("0")))
        )
        .order_by("account__account_name")
    )
    rows, total = [], Decimal("0")
    for rec in agg:
        bal = rec["deb"] - rec["cre"]           # debit-nature balance
        amt = bal if positive_is_debit else -bal
        if abs(amt) < Decimal("0.005"):
            continue
        rows.append((rec["account__account_name"], amt))
        total += amt
    return rows, total


def _apply_cash_basis_rules(asset_types, liab_types):
    """
    Very light cash-basis tweak: remove A/R and A/P buckets so they zero out.
    (QBO also affects other areas; refine here later if needed.)
    """
    aset = set(asset_types)
    liab = set(liab_types)
    aset -= {"accounts receivable"}
    liab -= {"accounts payable"}
    return aset, liab


def report_bs(request):
    """
    Balance Sheet (vertical, QBO-like).
    GET params:
      ?to=YYYY-MM-DD   -> 'As of' date
      ?method=cash|accrual  -> accounting method toggle
    """
    _, asof = _period(request)  # we only care about 'to'
    method = (request.GET.get("method") or "accrual").strip().lower()
    method = "cash" if method == "cash" else "accrual"

    # Journal lines up to 'as of'
    lines = _apply_asof(
        JournalLine.objects.select_related("account", "entry"),
        asof
    )

    # Cash-basis simplification: zero AR/AP by excluding those types
    asset_types = ASSET_TYPES
    liab_types  = LIAB_TYPES
    if method == "cash":
        asset_types, liab_types = _apply_cash_basis_rules(ASSET_TYPES, LIAB_TYPES)

    # Buckets
    asset_rows, asset_total = _net_by_types(lines, asset_types,  positive_is_debit=True)
    liab_rows,  liab_total  = _net_by_types(lines, liab_types,   positive_is_debit=False)
    eq_rows,    eq_total    = _net_by_types(lines, EQUITY_TYPES, positive_is_debit=False)

    # Retained earnings = cumulative net income up to 'as of'
    inc_pattern = _iregex_from_types(INCOME_TYPES)
    exp_pattern = _iregex_from_types(EXPENSE_TYPES)

    inc_val = (
        lines.filter(account__account_type__iregex=inc_pattern)
             .aggregate(v=Coalesce(Sum(F("credit") - F("debit")), Value(Decimal("0"))))["v"]
    )
    exp_val = (
        lines.filter(account__account_type__iregex=exp_pattern)
             .aggregate(v=Coalesce(Sum(F("debit") - F("credit")), Value(Decimal("0"))))["v"]
    )
    retained = inc_val - exp_val  # increases equity when positive

    eq_rows.append(("Retained Earnings", retained))
    eq_total = eq_total + retained

    # Company name (adjust to your source if you have one)
    company_name = getattr(getattr(request, "tenant", None), "name", "YoAccountant")

    return render(request, "balance_sheet.html", {
        "company_name": company_name,
        "method": method,
        "asset_rows": asset_rows, "asset_total": asset_total,
        "liab_rows": liab_rows,   "liab_total": liab_total,
        "eq_rows": eq_rows,       "eq_total": eq_total,
        "asof": asof,
        "check_ok": (asset_total == (liab_total + eq_total)),
    })
# working on the cashflow
# Account type buckets
INCOME_TYPES   = {"income", "other income"}
EXPENSE_TYPES  = {"expense", "other expense", "cost of goods sold"}
CASH_TYPES     = {"bank", "cash and cash equivalents"}
AR_TYPES       = {"accounts receivable"}
INV_TYPES      = {"inventory"}
AP_TYPES       = {"accounts payable"}
FIXED_ASSET_TYPES = {"fixed asset", "other asset"}
LOAN_TYPES        = {"current liability", "long term liability"}
EQUITY_TYPES      = {"equity"}

def _entry_date_field():
    """Detect whether JournalEntry uses entry_date or date."""
    Entry = JournalLine._meta.get_field("entry").remote_field.model
    try:
        Entry._meta.get_field("entry_date")
        return "entry_date"
    except FieldDoesNotExist:
        return "date"

def _apply_period(lines, dfrom, dto):
    """Apply range filter (inclusive) on detected entry date field."""
    df = _entry_date_field()
    if dfrom:
        lines = lines.filter(**{f"entry__{df}__gte": dfrom})
    if dto:
        lines = lines.filter(**{f"entry__{df}__lte": dto})
    return lines

def _iregex(type_set):  # case-insensitive regex from type names (safe)
    return "|".join(re.escape(t) for t in type_set)

def _ids_by_types(type_set):
    return list(
        Account.objects
        .filter(account_type__iregex=_iregex(type_set))
        .values_list("id", flat=True)
    )

def account_balance_asof(account_ids, asof):
    """Debit-nature balance (debit - credit) as of <= asof."""
    q = JournalLine.objects.filter(account_id__in=account_ids)
    if asof:
        df = _entry_date_field()
        q = q.filter(**{f"entry__{df}__lte": asof})
    agg = q.aggregate(
        deb=Coalesce(Sum("debit"),  Value(Decimal("0"))),
        cre=Coalesce(Sum("credit"), Value(Decimal("0")))
    )
    return agg["deb"] - agg["cre"]

def _change_in_balance(account_ids, dfrom, dto):
    """End balance minus balance just before the period start."""
    start_asof = (dfrom - timedelta(days=1)) if dfrom else None
    start_bal  = account_balance_asof(account_ids, start_asof)
    end_bal    = account_balance_asof(account_ids, dto)
    return end_bal - start_bal

def _net_profit_for_period(dfrom, dto):
    """Compute Net Profit for the period directly (no view calls)."""
    lines = _apply_period(
        JournalLine.objects.select_related("account", "entry"),
        dfrom, dto
    )
    inc = (
        lines.filter(account__account_type__iregex=_iregex(INCOME_TYPES))
             .aggregate(v=Coalesce(Sum(F("credit") - F("debit")), Value(Decimal("0"))))["v"]
    )
    exp = (
        lines.filter(account__account_type__iregex=_iregex(EXPENSE_TYPES))
             .aggregate(v=Coalesce(Sum(F("debit") - F("credit")), Value(Decimal("0"))))["v"]
    )
    return inc - exp  # profit positive

# ----- CASH FLOW (Indirect) -------------------------------------------
def report_cashflow(request):
    dfrom, dto = _period(request)

    # Net Profit
    net_profit = _net_profit_for_period(dfrom, dto)

    # Working capital changes (period deltas)
    delta_ar  = _change_in_balance(_ids_by_types(AR_TYPES),  dfrom, dto)  # ↑AR = cash outflow
    delta_inv = _change_in_balance(_ids_by_types(INV_TYPES), dfrom, dto)  # ↑Inv = cash outflow
    delta_ap  = _change_in_balance(_ids_by_types(AP_TYPES),  dfrom, dto)  # ↑AP  = cash inflow

    cash_from_ops = (
        net_profit
        - delta_ar        # increase AR reduces cash
        - delta_inv       # increase inventory reduces cash
        + delta_ap        # increase AP increases cash
    )

    # Investing (fixed assets etc): increase in FA = cash outflow
    delta_fa = _change_in_balance(_ids_by_types(FIXED_ASSET_TYPES), dfrom, dto)
    cash_from_investing = -delta_fa

    # Financing: increases in loans/equity are inflows
    delta_loans = _change_in_balance(_ids_by_types(LOAN_TYPES), dfrom, dto)
    delta_equity= _change_in_balance(_ids_by_types(EQUITY_TYPES), dfrom, dto)
    cash_from_financing = delta_loans + delta_equity

    # Net change in cash = sum sections
    net_change = cash_from_ops + cash_from_investing + cash_from_financing

    # Reconcile with cash/bank balances
    cash_ids   = _ids_by_types(CASH_TYPES)
    cash_start = account_balance_asof(cash_ids, (dfrom - timedelta(days=1)) if dfrom else None)
    cash_end   = account_balance_asof(cash_ids, dto)
    # Note: cash accounts are debit-nature; positive balance = cash asset.

    return render(request, "cashflow.html", {
        "dfrom": dfrom, "dto": dto,

        "net_profit": net_profit,
        "delta_ar": delta_ar,
        "delta_inv": delta_inv,
        "delta_ap": delta_ap,
        "cash_from_ops": cash_from_ops,

        "delta_fa": delta_fa,
        "cash_from_investing": cash_from_investing,

        "delta_loans": delta_loans,
        "delta_equity": delta_equity,
        "cash_from_financing": cash_from_financing,

        "net_change": net_change,
        "cash_start": cash_start,
        "cash_end": cash_end,
        "recon_ok": (cash_start + net_change == cash_end),
    })