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
from collections import defaultdict
from .models import JournalEntry, JournalLine
from django.utils import timezone
from decimal import Decimal
from datetime import timedelta
from django.core.exceptions import FieldDoesNotExist
from django.db.models import Sum, F, Value
from django.db.models.functions import Coalesce
from django.contrib.auth.decorators import login_required
from django.utils.dateparse import parse_date
from sowa_settings.models import CompanySettings,Currency

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

# working on the structured approach
# === Level 1 mapping for Chart of Accounts ============================
LEVEL1_ORDER = [
    "Assets",
    "Equity",
    "Liabilities",
    "Income",
    "Cost of Goods Sold",
    "Expense",
]

ACCOUNT_TYPE_TO_LEVEL1 = {
    # Assets
    "asset": "Assets",
    "assets": "Assets",
    "current assets": "Assets",
    "non-current assets": "Assets",
    "fixed assets": "Assets",
    "cash and cash equivalents": "Assets",
    "accounts receivable": "Assets",
    "inventory": "Assets",

    # Liabilities
    "liability": "Liabilities",
    "liabilities": "Liabilities",
    "current liabilities": "Liabilities",
    "non-current liabilities": "Liabilities",
    "loans": "Liabilities",

    # Equity
    "equity": "Equity",
    "owner's equity": "Equity",
    "owners equity": "Equity",
    "share capital": "Equity",

    # Income
    "income": "Income",
    "sales": "Income",
    "revenue": "Income",
    "other income": "Other Income",

    # COGS
    "cost of goods sold": "Cost of Goods Sold",

    # Expenses
    "expense": "Expenses",
    "expenses": "Expenses",
    "operating expenses": "Expenses",
    "other expense": "Other Expense",
    
}

@login_required
def accounts(request):
    status = request.GET.get("status", "active")  # default is active

    base_qs = Account.objects.all()

    # --- status filter for this view ---
    if status == "inactive":
        qs = base_qs.filter(is_active=False)
    elif status == "all":
        qs = base_qs
    else:  # "active"
        qs = base_qs.filter(is_active=True)

    qs = qs.select_related("parent").order_by("account_type", "account_name")

    # --- group accounts into Level 1 buckets using backend mapping ---
    grouped = {label: [] for label in LEVEL1_ORDER}

    for acc in qs:
        raw_type = (acc.account_type or "").strip().lower()

        level1 = ACCOUNT_TYPE_TO_LEVEL1.get(raw_type)

        # Fallbacks if someone creates weird/custom types
        if not level1:
            if "income" in raw_type:
                level1 = "Income"
            elif "cost of goods" in raw_type or "cogs" in raw_type:
                level1 = "Cost of Goods Sold"
            elif "expense" in raw_type:
                level1 = "Expenses"
            elif "liabil" in raw_type:
                level1 = "Liabilities"
            elif "equity" in raw_type:
                level1 = "Equity"
            else:
                level1 = "Assets"  # safe default

        grouped.setdefault(level1, []).append(acc)

    # ordered list for the template
    level1_sections = [
        {"label": label, "accounts": grouped.get(label, [])}
        for label in LEVEL1_ORDER
    ]

    # counts for badges
    active_count = base_qs.filter(is_active=True).count()
    inactive_count = base_qs.filter(is_active=False).count()
    all_count = base_qs.count()

    # Column preferences (same logic you had)
    if getattr(request.user, "is_authenticated", False):
        prefs, _ = ColumnPreference.objects.get_or_create(
            user=request.user,
            table_name="accounts",
            defaults={"preferences": DEFAULT_ACCOUNTS_COL_PREFS},
        )
        merged_prefs = {**DEFAULT_ACCOUNTS_COL_PREFS, **(prefs.preferences or {})}
    else:
        merged_prefs = DEFAULT_ACCOUNTS_COL_PREFS

    return render(request, "accounts.html", {
        "status": status,
        "column_prefs": merged_prefs,
        "active_count": active_count,
        "inactive_count": inactive_count,
        "all_count": all_count,
        # for the table
        "level1_sections": level1_sections,
        # optional: still pass qs if you want
        "coas": qs,
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
@login_required
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
def _company_context():
    """
    Fetch company name and reporting currency from the sowa_settings app.
    Adjust field names if your model is different.
    """
    settings_obj = CompanySettings.objects.first()
    if settings_obj:
        return {
            "company_name": getattr(settings_obj, "company_name", "") or "",
            "reporting_currency": getattr(settings_obj, "reporting_currency", "") or "",
        }
    return {
        "company_name": "",
        "reporting_currency": "",
    }

def _period(request):
    # ?from=2025-01-01&to=2025-12-31
    dfrom = parse_date(request.GET.get("from") or "")
    dto   = parse_date(request.GET.get("to") or "")
    return dfrom, dto

def _get_reporting_currency():
    code = "UGX"
    factor = Decimal("1")

    home = Currency.objects.filter(is_home=True).first()
    if home:
        code = home.code or code

    cs = CompanySettings.objects.first()

    cur = None
    if cs:
        rc = getattr(cs, "reporting_currency", None)

        if isinstance(rc, Currency):
            cur = rc
        elif isinstance(rc, str) and rc:
            cur = Currency.objects.filter(code__iexact=rc).first()

    if not cur:
        cur = home

    if not cur:
        return code, factor

    code = cur.code or code

    if home and cur.id == home.id:
        return code, factor

    if cur.rate_to_home and cur.rate_to_home != 0:
        factor = Decimal("1") / cur.rate_to_home

    return code, factor


@login_required
def trial_balance(request):
    dfrom = parse_date(request.GET.get("from", "") or "")
    dto   = parse_date(request.GET.get("to", "") or "")

    # Reporting currency & FX factor (UGX → reporting)
    reporting_currency, fx = _get_reporting_currency()

    lines = JournalLine.objects.select_related("entry", "account")

    if dfrom:
        lines = lines.filter(entry__date__gte=dfrom)
    if dto:
        lines = lines.filter(entry__date__lte=dto)

    agg = (
        lines.values("account_id", "account__account_name")
             .annotate(
                 debit = Coalesce(Sum("debit"),  Value(Decimal("0.00"), output_field=DecimalField())),
                 credit= Coalesce(Sum("credit"), Value(Decimal("0.00"), output_field=DecimalField())),
             )
             .order_by("account__account_name")
    )

    rows = []
    total_debit = total_credit = Decimal("0.00")

    for r in agg:
        # Original amounts in HOME currency (UGX)
        d_home = r["debit"]  or Decimal("0")
        c_home = r["credit"] or Decimal("0")

        # Convert to reporting currency
        d = d_home * fx
        c = c_home * fx

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
        "reporting_currency": reporting_currency,
    })
# working on the profits and losses

INCOME_TYPES  = {"income", "other income"}
EXPENSE_TYPES = {"expense", "other expense", "cost of goods sold"}


def _apply_entry_date_range(qs, dfrom, dto):
    """
    Apply date range to JournalLine queryset by discovering the
    correct date field on the related JournalEntry (e.g. 'entry_date' or 'date').
    """
    EntryModel = qs.model._meta.get_field("entry").remote_field.model
    date_field_name = "entry_date"
    try:
        EntryModel._meta.get_field("entry_date")
    except FieldDoesNotExist:
        date_field_name = "date"

    if dfrom:
        qs = qs.filter(**{f"entry__{date_field_name}__gte": dfrom})
    if dto:
        qs = qs.filter(**{f"entry__{date_field_name}__lte": dto})
    return qs

@login_required
def report_pnl(request):
    dfrom, dto = _period(request)

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

    # basic IFRS buckets (Operating section)
    buckets = {
        "income": [],   # Revenue
        "cogs":   [],   # Cost of goods sold
        "expense": []   # Operating expenses
    }
    totals = {
        "income": Decimal("0"),
        "cogs":   Decimal("0"),
        "expense": Decimal("0"),
    }

    for a in agg:
        t = (a["account__account_type"] or "").lower()
        rev_like = a["cre"] - a["deb"]   # revenue positive
        exp_like = a["deb"] - a["cre"]   # costs positive

        if t in INCOME_TYPES:
            buckets["income"].append((a["account__account_name"], rev_like))
            totals["income"] += rev_like
        elif t == "cost of goods sold":
            buckets["cogs"].append((a["account__account_name"], exp_like))
            totals["cogs"] += exp_like
        elif t in EXPENSE_TYPES:
            buckets["expense"].append((a["account__account_name"], exp_like))
            totals["expense"] += exp_like

    # IFRS subtotals
    gross_profit = totals["income"] - totals["cogs"]
    operating_profit = gross_profit - totals["expense"]

    # for now we don't yet split investing / financing / tax,
    # so these are equal to operating_profit
    profit_before_financing_tax = operating_profit
    profit_before_income_tax = operating_profit
    net_profit = profit_before_income_tax

    ctx = {
        "buckets": buckets,
        "totals": totals,
        "gross_profit": gross_profit,
        "operating_profit": operating_profit,
        "profit_before_financing_tax": profit_before_financing_tax,
        "profit_before_income_tax": profit_before_income_tax,
        "net_profit": net_profit,
        "dfrom": dfrom,
        "dto": dto,
    }
    ctx.update(_company_context())  # gives company_name, reporting_currency, etc.
    return render(request, "pnl.html", ctx)# working on the balance sheet
# P&L type sets used ONLY for retained earnings on the Balance Sheet
INCOME_TYPES  = {"income", "other income"}
EXPENSE_TYPES = {"expense", "other expense", "cost of goods sold"}

ASSET_CURRENT_TYPES = {
    "bank", "cash and cash equivalents", "current asset",
    "accounts receivable", "inventory", "prepaid expense", "other current assets",
}
ASSET_NONCURRENT_TYPES = {
    "fixed asset", "non-current asset", "other asset",
    "depletable assets", "land", "buildings", "machinery and equipment",
}

LIAB_CURRENT_TYPES = {"accounts payable", "current liability", "other current liabilities"}
LIAB_NONCURRENT_TYPES = {"non-current liability", "long term liability", "other non-current liabilities"}
EQUITY_TYPES  = {"equity", "owner's equity", "retained earnings"}


def _period_bs(request):
    """
    Small helper for Balance Sheet only, to avoid name clash with other _period.
    We mostly care about the 'to' date (as of).
    """
    dfrom = request.GET.get("from") or None
    dto   = request.GET.get("to") or None
    from datetime import datetime
    fmt = "%Y-%m-%d"
    try:
        dfrom = datetime.strptime(dfrom, fmt).date() if dfrom else None
    except Exception:
        dfrom = None
    try:
        dto   = datetime.strptime(dto, fmt).date() if dto else None
    except Exception:
        dto = None
    return dfrom, dto


def _apply_asof(qs, asof):
    """
    Filter JournalLines up to and including the 'as of' date
    on the related JournalEntry's date field (date or entry_date).
    """
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
    import re
    return "|".join(re.escape(t) for t in type_set)


def _bucket_balances(lines, type_set, positive_is_debit=True):
    """
    Returns (rows, total) for a bucket of account types.
    For assets: positive_is_debit=True  -> amount = debit - credit
    For liab/equity: positive_is_debit=False -> amount = credit - debit
    """
    pattern = _iregex_from_types(type_set)
    agg = (
        lines.filter(account__account_type__iregex=pattern)
             .values("account__account_name", "account__account_type")
             .annotate(
                 deb=Coalesce(Sum("debit"),  Value(Decimal("0"))),
                 cre=Coalesce(Sum("credit"), Value(Decimal("0"))),
             )
             .order_by("account__account_name")
    )
    rows, total = [], Decimal("0")
    for rec in agg:
        bal = rec["deb"] - rec["cre"]         # debit-nature balance
        amt = bal if positive_is_debit else -bal
        if abs(amt) < Decimal("0.005"):
            continue
        rows.append((rec["account__account_name"], amt))
        total += amt
    return rows, total


@login_required
def report_bs(request):
    """
    Statement of Financial Position (Balance Sheet).
    Layout:
      - Assets (Non-current, Current)
      - Equity & Liabilities:
          * Equity
          * Liabilities (Non-current, Current)
    """
    _, asof = _period_bs(request)
    method = (request.GET.get("method") or "accrual").strip().lower()
    method = "cash" if method == "cash" else "accrual"

    # Journal lines up to 'as of'
    lines = _apply_asof(
        JournalLine.objects.select_related("account", "entry"),
        asof
    )

    # Assets buckets
    asset_nc_rows,   asset_nc_total   = _bucket_balances(lines, ASSET_NONCURRENT_TYPES, positive_is_debit=True)
    asset_curr_rows, asset_curr_total = _bucket_balances(lines, ASSET_CURRENT_TYPES,   positive_is_debit=True)
    asset_total = asset_nc_total + asset_curr_total

    # Liabilities buckets
    liab_nc_rows,   liab_nc_total   = _bucket_balances(lines, LIAB_NONCURRENT_TYPES, positive_is_debit=False)
    liab_curr_rows, liab_curr_total = _bucket_balances(lines, LIAB_CURRENT_TYPES,    positive_is_debit=False)
    liab_total = liab_nc_total + liab_curr_total

    # Equity bucket
    eq_rows, eq_total = _bucket_balances(lines, EQUITY_TYPES, positive_is_debit=False)

    # Retained earnings from cumulative P&L balances
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
    retained = inc_val - exp_val

    eq_rows.append(("Retained Earnings", retained))
    eq_total = eq_total + retained

    ctx = {
        "asset_nc_rows": asset_nc_rows,
        "asset_nc_total": asset_nc_total,
        "asset_curr_rows": asset_curr_rows,
        "asset_curr_total": asset_curr_total,
        "asset_total": asset_total,

        "liab_nc_rows": liab_nc_rows,
        "liab_nc_total": liab_nc_total,
        "liab_curr_rows": liab_curr_rows,
        "liab_curr_total": liab_curr_total,
        "liab_total": liab_total,

        "eq_rows": eq_rows,
        "eq_total": eq_total,

        "asof": asof,
        "method": method,
        "check_ok": (asset_total == (liab_total + eq_total)),
    }
    ctx.update(_company_context())
    return render(request, "balance_sheet.html", ctx)


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

@login_required
def report_cashflow(request):
    dfrom, dto = _period(request)

    # Net Profit
    net_profit = _net_profit_for_period(dfrom, dto)

    # Working capital changes
    delta_ar  = _change_in_balance(_ids_by_types(AR_TYPES),  dfrom, dto)
    delta_inv = _change_in_balance(_ids_by_types(INV_TYPES), dfrom, dto)
    delta_ap  = _change_in_balance(_ids_by_types(AP_TYPES),  dfrom, dto)

    cash_from_ops = (
        net_profit
        - delta_ar
        - delta_inv
        + delta_ap
    )

    # Investing
    delta_fa = _change_in_balance(_ids_by_types(FIXED_ASSET_TYPES), dfrom, dto)
    cash_from_investing = -delta_fa

    # Financing
    delta_loans  = _change_in_balance(_ids_by_types(LOAN_TYPES),  dfrom, dto)
    delta_equity = _change_in_balance(_ids_by_types(EQUITY_TYPES), dfrom, dto)
    cash_from_financing = delta_loans + delta_equity

    net_change = cash_from_ops + cash_from_investing + cash_from_financing

    cash_ids   = _ids_by_types(CASH_TYPES)
    cash_start = account_balance_asof(cash_ids, (dfrom - timedelta(days=1)) if dfrom else None)
    cash_end   = account_balance_asof(cash_ids, dto)

    ctx = {
        "dfrom": dfrom,
        "dto": dto,
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
    }
    ctx.update(_company_context())
    return render(request, "cashflow.html", ctx)