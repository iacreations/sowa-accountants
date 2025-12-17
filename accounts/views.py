import csv
from collections import OrderedDict
from django.core.paginator import Paginator
from decimal import Decimal
import json
from datetime import datetime
from django.db.models import Q, Sum
from django.utils.dateparse import parse_date
from django.db.models import Sum, Value, DecimalField
from django.db.models.functions import Coalesce
from django.urls import reverse
from django.http import HttpResponse,Http404
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.db import transaction
from django.shortcuts import render, redirect, get_object_or_404
from django.utils import timezone
from .models import (Account, ColumnPreference, JournalEntry, JournalLine, AuditTrail)


# default visible columns (match the checkboxes in the template)
DEFAULT_ACCOUNTS_COL_PREFS = {
    "account_name": True,
    "opening_balance": True,
    "as_of": True,
    "account_number": True,
    "detail_type": True,
    "description": True,
    "actions": True,
}

# fixed 5 Level-1 sections
LEVEL1_ORDER = [
    "Assets",
    "Equity",
    "Liabilities",
    "Income",
    "Expenses",
]

CASH_DETAIL_TYPES = [
    "Cash and Cash equivalents",
    # later you can add:
    # "Petty Cash",
    # "Bank current account",
    # "Mobile Money",
]
# @login_required
def accounts(request):
    status = request.GET.get("status", "active")  # default is active

    base_qs = Account.objects.all()

    # status filter
    if status == "inactive":
        qs = base_qs.filter(is_active=False)
    elif status == "all":
        qs = base_qs
    else:  # "active"
        qs = base_qs.filter(is_active=True)

    qs = qs.select_related("parent").order_by("account_type", "account_name")

    # group into 5 Level-1 buckets using model property
    grouped = {label: [] for label in LEVEL1_ORDER}

    for acc in qs:
        level1 = acc.level1_group or "Assets"  # safe default if missing
        grouped.setdefault(level1, []).append(acc)

    level1_sections = [
        {"label": label, "accounts": grouped.get(label, [])}
        for label in LEVEL1_ORDER
    ]

    # counts for tabs
    active_count = base_qs.filter(is_active=True).count()
    inactive_count = base_qs.filter(is_active=False).count()
    all_count = base_qs.count()

    # Column preferences
    if request.user.is_authenticated:
        prefs, _ = ColumnPreference.objects.get_or_create(
            user=request.user,
            table_name="accounts",
            defaults={"preferences": DEFAULT_ACCOUNTS_COL_PREFS},
        )
        merged_prefs = {**DEFAULT_ACCOUNTS_COL_PREFS, **(prefs.preferences or {})}
    else:
        merged_prefs = DEFAULT_ACCOUNTS_COL_PREFS

    return render(
        request,
        "accounts.html",
        {
            "status": status,
            "column_prefs": merged_prefs,
            "active_count": active_count,
            "inactive_count": inactive_count,
            "all_count": all_count,
            "level1_sections": level1_sections,
            "coas": qs,
        },
    )

# audit trail view
def audit_trail(request):
    logs = AuditTrail.objects.select_related("user").all()

    return render(request, "audit_trail.html", {
        "logs": logs
    })



# @login_required
def save_column_prefs(request):
    if request.method != "POST":
        return JsonResponse(
            {"status": "error", "detail": "POST required"}, status=400
        )

    try:
        data = json.loads(request.body or "{}")
        preferences = data.get("preferences", {})
    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "detail": "Bad JSON"}, status=400)

    prefs, _ = ColumnPreference.objects.get_or_create(
        user=request.user,
        table_name="accounts",
        defaults={"preferences": DEFAULT_ACCOUNTS_COL_PREFS},
    )

    # ensure only known keys are saved
    cleaned = {
        k: bool(preferences.get(k, True)) for k in DEFAULT_ACCOUNTS_COL_PREFS.keys()
    }

    prefs.preferences = cleaned
    prefs.save()
    return JsonResponse({"status": "ok"})

# ading an account in the coa
# @login_required
@transaction.atomic
def add_account(request):
    if request.method == "POST":
        account_name   = request.POST.get("account_name")
        account_number = request.POST.get("account_number")
        account_type   = request.POST.get("account_type")  # code from select
        detail_type    = request.POST.get("detail_type")
        tax_category   = request.POST.get("tax_category")
        is_subaccount  = request.POST.get("is_subaccount") == "on"

        parent = None
        parent_id = request.POST.get("parent")
        if is_subaccount and parent_id:
            parent = Account.objects.filter(id=parent_id).first()

        opening_balance_str = request.POST.get("opening_balance") or "0"
        try:
            opening_balance = Decimal(opening_balance_str)
        except Exception:
            opening_balance = Decimal("0")

        as_of_str = request.POST.get("as_of")
        as_of = as_of_str or timezone.now().date()

        description = request.POST.get("description")

        # 1) Save the account
        new_account = Account(
            account_name=account_name,
            account_number=account_number,
            account_type=account_type,
            detail_type=detail_type,
            tax_category=tax_category,
            is_subaccount=is_subaccount,
            parent=parent,
            opening_balance=opening_balance,
            as_of=as_of,
            description=description,
        )
        new_account.save()

        # 2) Opening balance JE (same as you had)
        if opening_balance != 0:
            level1 = new_account.level1_group  # Assets / Liabilities / Equity / Income / Expenses

            if level1 in ["Assets", "Liabilities", "Equity"]:
                opening_equity_acct, _ = Account.objects.get_or_create(
                    account_name="Opening Balance Equity",
                    account_type="OWNER_EQUITY",
                    defaults={
                        "detail_type": "Opening balances",
                        "is_active": True,
                    },
                )

                je = JournalEntry.objects.create(
                    date=as_of,
                    description=f"Opening balance for {new_account.account_name}",
                    source_type="OPENING_BALANCE",
                    source_id=new_account.id,
                )

                amount = abs(opening_balance)

                if level1 == "Assets":
                    # Debit the asset account, Credit Opening Balance Equity
                    JournalLine.objects.create(
                        entry=je,
                        account=new_account,
                        debit=amount,
                        credit=Decimal("0"),
                    )
                    JournalLine.objects.create(
                        entry=je,
                        account=opening_equity_acct,
                        debit=Decimal("0"),
                        credit=amount,
                    )
                else:
                    # Liabilities & Equity: Credit account, Debit Opening Balance Equity
                    JournalLine.objects.create(
                        entry=je,
                        account=new_account,
                        debit=Decimal("0"),
                        credit=amount,
                    )
                    JournalLine.objects.create(
                        entry=je,
                        account=opening_equity_acct,
                        debit=amount,
                        credit=Decimal("0"),
                    )

        # 3) Redirect as before
        save_action = request.POST.get("save_action")
        if save_action == "save&new":
            return redirect("accounts:add-account")
        elif save_action == "save&close":
            return redirect("accounts:accounts")

        return redirect("accounts:accounts")

    # GET
    parents = Account.objects.all()
    return render(request, "coa_form.html", {
        "parents": parents,
        "account": None,      # important so template knows this is ADD mode
    })


@transaction.atomic
def edit_account(request, pk):
    """
    Edit an existing Account including its opening balance journal entry.
    """
    account = get_object_or_404(Account, pk=pk)

    if request.method == "POST":
        account_name   = request.POST.get("account_name")
        account_number = request.POST.get("account_number")
        account_type   = request.POST.get("account_type")
        detail_type    = request.POST.get("detail_type")
        tax_category   = request.POST.get("tax_category")
        is_subaccount  = request.POST.get("is_subaccount") == "on"

        parent = None
        parent_id = request.POST.get("parent")
        if is_subaccount and parent_id:
            parent = Account.objects.filter(id=parent_id).first()

        opening_balance_str = request.POST.get("opening_balance") or "0"
        try:
            new_opening_balance = Decimal(opening_balance_str)
        except Exception:
            new_opening_balance = Decimal("0")

        as_of_str = request.POST.get("as_of")
        as_of = as_of_str or timezone.now().date()

        description = request.POST.get("description")

        # ---- update account fields ----
        account.account_name   = account_name
        account.account_number = account_number
        account.account_type   = account_type
        account.detail_type    = detail_type
        account.tax_category   = tax_category
        account.is_subaccount  = is_subaccount
        account.parent         = parent
        account.opening_balance = new_opening_balance
        account.as_of          = as_of
        account.description    = description
        account.save()

        # ---- update opening balance journal entry ----
        # Only for balance-sheet accounts
        level1 = account.level1_group
        je = JournalEntry.objects.filter(
            source_type="OPENING_BALANCE",
            source_id=account.id
        ).first()

        if level1 not in ["Assets", "Liabilities", "Equity"]:
            # Not a balance-sheet account: remove any OB entry
            if je:
                je.delete()
        else:
            if new_opening_balance == 0:
                # zero balance: remove any OB entry
                if je:
                    je.delete()
            else:
                # Ensure Opening Balance Equity exists
                opening_equity_acct, _ = Account.objects.get_or_create(
                    account_name="Opening Balance Equity",
                    account_type="OWNER_EQUITY",
                    defaults={
                        "detail_type": "Opening balances",
                        "is_active": True,
                    },
                )

                amount = abs(new_opening_balance)

                if not je:
                    je = JournalEntry.objects.create(
                        date=as_of,
                        description=f"Opening balance for {account.account_name}",
                        source_type="OPENING_BALANCE",
                        source_id=account.id,
                    )
                else:
                    je.date = as_of
                    je.description = f"Opening balance for {account.account_name}"
                    je.save()
                    # wipe existing lines and rebuild
                    JournalLine.objects.filter(entry=je).delete()

                if level1 == "Assets":
                    # DR asset, CR Opening Balance Equity
                    JournalLine.objects.create(
                        entry=je,
                        account=account,
                        debit=amount,
                        credit=Decimal("0"),
                    )
                    JournalLine.objects.create(
                        entry=je,
                        account=opening_equity_acct,
                        debit=Decimal("0"),
                        credit=amount,
                    )
                else:
                    # Liabilities/Equity: CR account, DR Opening Balance Equity
                    JournalLine.objects.create(
                        entry=je,
                        account=account,
                        debit=Decimal("0"),
                        credit=amount,
                    )
                    JournalLine.objects.create(
                        entry=je,
                        account=opening_equity_acct,
                        debit=amount,
                        credit=Decimal("0"),
                    )

        # redirects
        save_action = request.POST.get("save_action")
        if save_action == "save&new":
            return redirect("accounts:add-account")
        elif save_action == "save&close":
            return redirect("accounts:accounts")

        return redirect("accounts:accounts")

    # GET: show form with existing values
    parents = Account.objects.exclude(pk=account.pk)
    return render(request, "coa_form.html", {
        "parents": parents,
        "account": account,
    })
# working on the activate and make inactive 
# @login_required
def deactivate_account(request, pk):
    coa = get_object_or_404(Account, pk=pk)
    coa.is_active = False
    coa.save()
    return redirect("accounts:accounts")


# @login_required
def activate_account(request, pk):
    coa = get_object_or_404(Account, pk=pk)
    coa.is_active = True
    coa.save()
    return redirect("accounts:accounts")

# the general ledger logic
# making the general ledger rows linkable

def get_entry_link(entry):
    """
    Map a JournalEntry to the EDIT page of the original transaction.
    Falls back to a journal entry edit/detail view if we don't know the source.
    """

    # normalise source_type to uppercase to handle both 'expense'/'EXPENSE'
    st = (entry.source_type or "").upper()
    sid = entry.source_id

    # Opening balances from Chart of Accounts -> edit that account
    if st == "OPENING_BALANCE":
        if sid:
            # sid is the Account PK we stored when creating the JE
            return reverse("accounts:edit-account", args=[sid])
        # fallback if somehow source_id is missing
        return reverse("accounts:accounts")
    
        # ---- CUSTOMER OPENING BALANCE ---------------------------------------
    if st == "CUSTOMER_OPENING_BALANCE" and sid:
        return reverse("sowaf:edit-customer", args=[sid])


    # ---- SALES / INVOICES -------------------------------------------------
    if st == "INVOICE" and sid:
        # if you have an invoice-edit view, prefer that:
        try:
            return reverse("sales:edit-invoice", args=[sid])
        except Exception:
            # fall back to the detail view you already had wired
            return reverse("sales:invoice-detail", args=[sid])
# ---- SALES / PAYMENTS -------------------------------------------------
    if st == "PAYMENT" and sid:
        # if you have an payment-edit view, prefer that:
        try:
            return reverse("sales:payment-edit", args=[sid])
        except Exception:
            # fall back to the detail view you already had wired
            return reverse("sales:payment-detail", args=[sid])
# ---- SALES / RECEIPTS -------------------------------------------------
    if st == "SALES_RECEIPT" and sid:
        # if you have an receipt-edit view, prefer that:
        try:
            return reverse("sales:receipt-edit", args=[sid])
        except Exception:
            # fall back to the detail view you already had wired
            return reverse("sales:receipt-detail", args=[sid])

    # ---- EXPENSES MODULE ---------------------------------------------------
    # Bill
    if st == "BILL" and sid:
        return reverse("expenses:bill-edit", args=[sid])

    # Expense
    if st == "EXPENSE" and sid:
        return reverse("expenses:expense-edit", args=[sid])

    # Cheque
    if st == "CHEQUE" and sid:
        return reverse("expenses:cheque-edit", args=[sid])

    # Purchase Order
    if st == "PURCHASE_ORDER" and sid:
        return reverse("expenses:purchase-order-edit", args=[sid])

    # Supplier Credit
    if st == "SUPPLIER_CREDIT" and sid:
        return reverse("expenses:supplier-credit-edit", args=[sid])

    # Pay Down Credit Card
    if st == "PAYDOWN_CREDIT" and sid:
        return reverse("expenses:paydown-credit-edit", args=[sid])

    # Credit Card Credit
    if st == "CREDIT_CARD_CREDIT" and sid:
        return reverse("expenses:credit-card-credit-edit", args=[sid])

    # ---- FALLBACK: MANUAL JOURNALS ---------------------------------------
    # If we get here, either source_type is empty/unknown,
    # or we couldn't reverse a URL above.
    try:
        # if you have an edit view for journal entries
        return reverse("accounts:journal-entry-edit", args=[entry.id])
    except Exception:
        try:
            # fall back to your old detail view if edit doesn't exist
            return reverse("accounts:journal-entry-detail", args=[entry.id])
        except Exception:
            # last resort – no link, but don't break the GL page
            return "#" 

# @login_required
def general_ledger(request):
    # filters
    account_id = request.GET.get("account_id")
    query      = request.GET.get("search", "")
    date_from  = request.GET.get("date_from")
    date_to    = request.GET.get("date_to")
    export     = request.GET.get("export")     # 'excel' or None
    page_num   = request.GET.get("page", 1)

    accounts = Account.objects.filter(is_active=True).order_by("account_name")

    lines_qs = JournalLine.objects.select_related("entry", "account").order_by(
        "-entry__date",
        "-id"
    )

    if account_id:
        lines_qs = lines_qs.filter(account_id=account_id)

    if date_from:
        lines_qs = lines_qs.filter(entry__date__gte=date_from)
    if date_to:
        lines_qs = lines_qs.filter(entry__date__lte=date_to)

    if query:
        lines_qs = lines_qs.filter(
            Q(entry__description__icontains=query) |
            Q(account__account_name__icontains=query)
        )

    # evaluate once so we can paginate & also compute monthly totals
    all_lines = list(lines_qs)

    # ====== EXPORT TO EXCEL (CSV) ======
    if export == "excel":
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="general_ledger.csv"'

        writer = csv.writer(response)
        writer.writerow(["Date", "Description", "Account", "Journal Ref", "Debit", "Credit", "Balance"])

        balance = Decimal("0")

        def normal_side(acc: Account):
            if acc.level1_group in ["Assets", "Expenses"]:
                return "debit"
            return "credit"

        for ln in all_lines:
            side = normal_side(ln.account)
            if side == "debit":
                balance += ln.debit - ln.credit
            else:
                balance += ln.credit - ln.debit

            #Rename in export too
            desc = ln.entry.description or ""
            if desc.lower().startswith("payment"):
                desc = desc.replace("Payment", "Sales Collection", 1)

            writer.writerow([
                ln.entry.date.strftime("%Y-%m-%d"),
                desc,
                ln.account.account_name,
                ln.entry.id,
                float(ln.debit),
                float(ln.credit),
                float(balance),
            ])

        return response

    # ====== MONTHLY TOTALS (for filtered data) ======
    monthly_totals = OrderedDict()
    for ln in all_lines:
        key = ln.entry.date.strftime("%Y-%m")
        label = ln.entry.date.strftime("%b %Y")
        if key not in monthly_totals:
            monthly_totals[key] = {"label": label, "debit": Decimal("0"), "credit": Decimal("0")}
        monthly_totals[key]["debit"]  += ln.debit
        monthly_totals[key]["credit"] += ln.credit

    for key, mt in monthly_totals.items():
        mt["balance"] = mt["debit"] - mt["credit"]

    # ====== PAGINATION ======
    paginator = Paginator(all_lines, 50)
    page_obj  = paginator.get_page(page_num)
    page_lines = page_obj.object_list

    running_rows = []
    balance = Decimal("0")

    def normal_side(acc: Account):
        if acc.level1_group in ["Assets", "Expenses"]:
            return "debit"
        return "credit"

    total_debit = Decimal("0")
    total_credit = Decimal("0")

    for ln in page_lines:
        side = normal_side(ln.account)

        if side == "debit":
            balance += ln.debit - ln.credit
        else:
            balance += ln.credit - ln.debit

        total_debit += ln.debit
        total_credit += ln.credit

        #FINAL DISPLAY RENAMING (Ledger UI)
        description = ln.entry.description or ""
        if description.lower().startswith("payment"):
            description = description.replace("Payment", "Sales Collection", 1)

        running_rows.append({
            "date": ln.entry.date,
            "description": description,
            "account": ln.account.account_name,
            "reference": ln.entry.id,
            "debit": ln.debit,
            "credit": ln.credit,
            "balance": balance,
            "url": get_entry_link(ln.entry),
        })

    return render(request, "general_ledger.html", {
        "accounts": accounts,
        "rows": running_rows,
        "page_obj": page_obj,
        "total_debit": total_debit,
        "total_credit": total_credit,
        "total_balance": total_debit - total_credit,
        "monthly_totals": monthly_totals.values(),
        "query": query,
        "account_id": account_id,
        "date_from": date_from,
        "date_to": date_to,
    })

# journel entry lists
# @login_required
def journal_entries(request):
    entries = (
        JournalEntry.objects
        .prefetch_related("lines__account")
        .order_by("-date", "-id")
    )

    totals = JournalLine.objects.filter(entry__in=entries).aggregate(
        grand_debit=Sum("debit"),
        grand_credit=Sum("credit"),
    )

    # ADD EDIT URLs JUST LIKE GL
    for entry in entries:
        entry.edit_url = get_entry_link(entry)

    context = {
        "entries": entries,
        "grand_debit": totals.get("grand_debit") or Decimal("0"),
        "grand_credit": totals.get("grand_credit") or Decimal("0"),
    }

    return render(request, "journal_entries.html", context)
# journal entry detail view
# @login_required
def journal_entry_detail(request, pk):
    entry = (
        JournalEntry.objects
        .prefetch_related("lines__account")
        .filter(pk=pk)
        .first()
    )
    if not entry:
        raise Http404("Journal entry not found")

    totals = JournalLine.objects.filter(entry=entry).aggregate(
        grand_debit=Sum("debit"),
        grand_credit=Sum("credit"),
    )
     
    context = {
        "entries": [entry],  # reuse the same table layout
        "grand_debit": totals.get("grand_debit") or Decimal("0"),
        "grand_credit": totals.get("grand_credit") or Decimal("0"),
    }
    return render(request, "journal_entries.html", context)
# print view 


# @login_required
def general_ledger_print(request):
    """
    Clean print-friendly General Ledger view.
    No pagination, no export, no UI – just the report.
    """

    # same filters as normal GL
    account_id = request.GET.get("account_id")
    query      = request.GET.get("search", "")
    date_from  = request.GET.get("date_from")
    date_to    = request.GET.get("date_to")

    selected_account = None
    if account_id:
        selected_account = Account.objects.filter(pk=account_id).first()

    lines_qs = JournalLine.objects.select_related("entry", "account").order_by(
        "entry__date",
        "id"
    )

    if account_id:
        lines_qs = lines_qs.filter(account_id=account_id)

    if date_from:
        lines_qs = lines_qs.filter(entry__date__gte=date_from)
    if date_to:
        lines_qs = lines_qs.filter(entry__date__lte=date_to)

    if query:
        lines_qs = lines_qs.filter(
            Q(entry__description__icontains=query) |
            Q(account__account_name__icontains=query)
        )

    lines = list(lines_qs)

    # compute running balance + totals (no pagination here)
    running_rows = []
    balance = Decimal("0")
    total_debit = Decimal("0")
    total_credit = Decimal("0")

    def normal_side(acc: Account):
        if acc.level1_group in ["Assets", "Expenses"]:
            return "debit"
        return "credit"

    for ln in lines:
        side = normal_side(ln.account)
        if side == "debit":
            balance += ln.debit - ln.credit
        else:
            balance += ln.credit - ln.debit

        total_debit += ln.debit
        total_credit += ln.credit

        running_rows.append({
            "date": ln.entry.date,
            "description": ln.entry.description,
            "account": ln.account.account_name,
            "reference": ln.entry.id,
            "debit": ln.debit,
            "credit": ln.credit,
            "balance": balance,
        })

    context = {
        "rows": running_rows,
        "total_debit": total_debit,
        "total_credit": total_credit,
        "total_balance": total_debit - total_credit,
        "selected_account": selected_account,
        "date_from": date_from,
        "date_to": date_to,
        "query": query,
    }
    return render(request, "general_ledger_print.html", context)

# working on the reports 

def _parse_range(request):
    """
    Helper to parse ?from=YYYY-MM-DD&to=YYYY-MM-DD from query string.
    Returns (dfrom, dto) as date or None.
    """
    from_str = request.GET.get("from") or request.GET.get("date_from")
    to_str   = request.GET.get("to") or request.GET.get("date_to")

    dfrom = parse_date(from_str) if from_str else None
    dto   = parse_date(to_str) if to_str else None
    return dfrom, dto


def report_trial_balance(request):
    """
    Trial Balance = sum of debits & credits per account over a period,
    straight from JournalLine.
    """
    dfrom, dto = _parse_range(request)

    lines = JournalLine.objects.select_related("entry", "account")

    if dfrom:
        lines = lines.filter(entry__date__gte=dfrom)
    if dto:
        lines = lines.filter(entry__date__lte=dto)

    # group by account
    agg = (
        lines
        .values("account_id", "account__account_name")
        .annotate(
            debit=Coalesce(Sum("debit"),  Value(Decimal("0.00")), output_field=DecimalField(max_digits=18, decimal_places=2)),
            credit=Coalesce(Sum("credit"), Value(Decimal("0.00")), output_field=DecimalField(max_digits=18, decimal_places=2)),
        )
        .order_by("account__account_name")
    )

    rows = []
    total_debit = Decimal("0.00")
    total_credit = Decimal("0.00")

    for r in agg:
        d = r["debit"] or Decimal("0.00")
        c = r["credit"] or Decimal("0.00")

        rows.append({
            "account_id": r["account_id"],
            "account": r["account__account_name"],
            "debit": d,
            "credit": c,
        })
        total_debit += d
        total_credit += c

    context = {
        "company_name": "YoAccountant",  # or pull from a Company model if you have one
        "reporting_currency": "UGX",      # adjust if you store this elsewhere
        "rows": rows,
        "total_debit": total_debit,
        "total_credit": total_credit,
        "dfrom": dfrom,
        "dto": dto,
    }
    return render(request, "trial_balance.html", context)

# profit and loss
# Treat these detail types as COGS (from your COA form options)
COGS_DETAIL_TYPES = {
    "cost of goods sold",
    "cost of sales",
}


def dec(v) -> Decimal:
    """Safe Decimal converter."""
    try:
        return Decimal(str(v or "0"))
    except Exception:
        return Decimal("0.00")


def bankish_q():
    """
    IMPORTANT: Use DETAIL TYPE (your COA has 3 layers).
    If your bank accounts have detail_type like 'Bank', 'Cash and Cash Equivalents', etc.
    this will catch them.
    """
    return (
        Q(detail_type__icontains="bank") |
        Q(detail_type__icontains="cash") |
        Q(detail_type__icontains="cash and cash equivalents") |
        Q(detail_type__icontains="cash on hand")
    )


# Your COA codes (recommended) + fallback text matching
INCOME_TYPES = {"OPERATING_INCOME", "INVESTING_INCOME"}
EXPENSE_TYPES = {"OPERATING_EXPENSE", "INVESTING_EXPENSE", "FINANCING_EXPENSE", "INCOME_TAX_EXPENSE"}


def report_pnl(request):
    # ---------- 1. Parse date filters ----------
    dfrom_raw = request.GET.get("from")
    dto_raw = request.GET.get("to")

    date_from = None
    date_to = None

    try:
        if dfrom_raw:
            date_from = datetime.strptime(dfrom_raw, "%Y-%m-%d").date()
        if dto_raw:
            date_to = datetime.strptime(dto_raw, "%Y-%m-%d").date()
    except ValueError:
        date_from = None
        date_to = None

    # ---------- 2. Accounting basis toggle (cash/accrual) ----------
    basis = (request.GET.get("basis") or "accrual").lower()
    if basis not in {"cash", "accrual"}:
        basis = "accrual"

    # ---------- 3. Base queryset from journal lines ----------
    jl = JournalLine.objects.select_related("account", "entry")

    if date_from:
        jl = jl.filter(entry__date__gte=date_from)
    if date_to:
        jl = jl.filter(entry__date__lte=date_to)

    # ---------- 4. Cash basis filter (QB-like simplified) ----------
    # Keep only entries that touch cash/bank, then P&L lines in those entries count.
    if basis == "cash":
        cash_bank_accounts = Account.objects.filter(is_active=True).filter(bankish_q())

        cash_entry_ids = (
            jl.filter(account__in=cash_bank_accounts)
              .values_list("entry_id", flat=True)
              .distinct()
        )

        jl = jl.filter(entry_id__in=cash_entry_ids)

    # Only accounts that actually have postings in this period (after basis filtering)
    accounts = (
        Account.objects
        .filter(journalline__in=jl)
        .distinct()
    )

    # ---------- 5. Helper: classify accounts into P&L buckets ----------
    def classify_account(acc: Account) -> str | None:
        """
        Returns 'income', 'cogs', 'expense', or None (ignore).
        Uses COA codes first, then falls back to text matching.
        """
        code = (acc.account_type or "").strip()
        detail = (getattr(acc, "detail_type", "") or "").strip().lower()
        name = (acc.account_name or "").strip().lower()

        # Strong by code
        if code in INCOME_TYPES:
            return "income"
        if code in EXPENSE_TYPES:
            # Some expenses are really COGS (optional split)
            if detail in COGS_DETAIL_TYPES or any(k in name for k in ["cogs", "cost of sales", "cost of goods"]):
                return "cogs"
            return "expense"

        # Fallback by text (in case some accounts were created with unexpected codes)
        text = f"{(acc.account_type or '').lower()} {detail} {name}"

        if "income" in text or "revenue" in text or "sales" in text:
            return "income"
        if "cogs" in text or "cost of goods" in text or "cost of sales" in text:
            return "cogs"
        if "expense" in text:
            return "expense"

        return None

    # ---------- 6. Build buckets ----------
    buckets = {"income": [], "cogs": [], "expense": []}
    totals = {
        "income": Decimal("0.00"),
        "cogs": Decimal("0.00"),
        "expense": Decimal("0.00"),
    }

    for acc in accounts:
        bucket = classify_account(acc)
        if not bucket:
            continue

        agg = jl.filter(account=acc).aggregate(
            debit=Sum("debit"),
            credit=Sum("credit"),
        )
        debit = dec(agg["debit"])
        credit = dec(agg["credit"])

        # Income accounts: normally credit balance → credit - debit
        # Expense / COGS: normally debit balance → debit - credit
        if bucket == "income":
            amount = credit - debit
        else:
            amount = debit - credit

        if amount == 0:
            continue

        buckets[bucket].append({
            "account": acc.account_name,
            "account_id": acc.id,
            "amount": amount,
        })
        totals[bucket] += amount

    # ---------- 7. Totals ----------
    gross_profit = totals["income"] - totals["cogs"]
    operating_profit = gross_profit - totals["expense"]
    net_profit = operating_profit

    context = {
        "company_name": "YoAccountant",
        "reporting_currency": "UGX",

        "basis": basis,  # ✅ send to template

        "buckets": buckets,
        "totals": totals,
        "gross_profit": gross_profit,
        "operating_profit": operating_profit,
        "profit_before_financing_tax": operating_profit,
        "profit_before_income_tax": operating_profit,
        "net_profit": net_profit,

        "dfrom": date_from,
        "dto": date_to,
    }

    return render(request, "pnl.html", context)

# balance sheet

def report_balance_sheet(request):
    """
    Balance sheet 'as of' a single date, grouped into
    non-current/current assets & liabilities, with each
    account clickable to the General Ledger.

    Adds method toggle:
    - accrual: use all JournalLines up to 'asof'
    - cash:    use ONLY JournalLines that hit bank/cash accounts (detail_type contains cash/bank)
    """

    # 1) Read filters
    to_str = request.GET.get("to")
    method = request.GET.get("method") or "accrual"

    if to_str:
        try:
            asof = datetime.strptime(to_str, "%Y-%m-%d").date()
        except ValueError:
            asof = timezone.localdate()
    else:
        asof = timezone.localdate()

    # ---- helper: what counts as cash/bank in your 3-level COA ----
    def bankish_q():
        return (
            Q(account__detail_type__icontains="bank") |
            Q(account__detail_type__icontains="cash") |
            Q(account__detail_type__icontains="cash and cash equivalents") |
            Q(account__detail_type__icontains="cash on hand")
        )

    # 2) All lines up to that date
    base_lines = JournalLine.objects.select_related("entry", "account").filter(entry__date__lte=asof)

    # 2b) CASH method: only keep lines that hit bank/cash accounts
    if method == "cash":
        base_lines = base_lines.filter(bankish_q())

    # 3) Aggregate per account
    per_account = (
        base_lines
        .values(
            "account_id",
            "account__account_name",
            "account__account_type",
            "account__detail_type",
        )
        .annotate(
            total_debit=Sum("debit"),
            total_credit=Sum("credit"),
        )
    )

    # Containers for rows
    asset_nc_rows = []
    asset_curr_rows = []
    eq_rows = []
    liab_nc_rows = []
    liab_curr_rows = []

    def infer_is_current(acc_type_code: str, detail: str) -> bool:
        """
        Very simple heuristic to split current vs non-current.
        """
        detail_l = (detail or "").lower()

        is_current_local = True

        if "non current" in detail_l or "non-current" in detail_l:
            is_current_local = False
        if any(k in detail_l for k in ["fixed", "property", "equipment", "long term", "long-term", "ppe"]):
            is_current_local = False

        return is_current_local

    # 4) Classify accounts + compute balances
    for row in per_account:
        code = row["account__account_type"] or ""
        level1 = Account.ACCOUNT_LEVEL1_MAP.get(code, "")
        detail_type = row.get("account__detail_type") or ""

        debit = row["total_debit"] or 0
        credit = row["total_credit"] or 0
        net = Decimal(debit) - Decimal(credit)  # debit-positive convention

        if level1 == "Assets":
            amount = net  # assets are debit balances
            is_current = infer_is_current(code, detail_type)
            target_list = asset_curr_rows if is_current else asset_nc_rows

        elif level1 == "Liabilities":
            amount = -net  # liabilities are credit balances
            is_current = infer_is_current(code, detail_type)
            target_list = liab_curr_rows if is_current else liab_nc_rows

        elif level1 == "Equity":
            amount = -net  # equity is credit balance
            target_list = eq_rows

        else:
            # ignore income/expense here; they flow via retained earnings
            continue

        if not amount:
            continue

        target_list.append({
            "account": row["account__account_name"],
            "account_id": row["account_id"],
            "amount": amount,
        })

    # 5) Totals
    asset_nc_total = sum(r["amount"] for r in asset_nc_rows) if asset_nc_rows else Decimal("0.00")
    asset_curr_total = sum(r["amount"] for r in asset_curr_rows) if asset_curr_rows else Decimal("0.00")
    asset_total = asset_nc_total + asset_curr_total

    liab_nc_total = sum(r["amount"] for r in liab_nc_rows) if liab_nc_rows else Decimal("0.00")
    liab_curr_total = sum(r["amount"] for r in liab_curr_rows) if liab_curr_rows else Decimal("0.00")
    liab_total = liab_nc_total + liab_curr_total

    eq_total = sum(r["amount"] for r in eq_rows) if eq_rows else Decimal("0.00")

    # 6) Balance check
    check_ok = round(asset_total, 2) == round(liab_total + eq_total, 2)

    # Nice label for header
    method_label = "Cash basis" if method == "cash" else "Accrual basis"

    context = {
        "company_name": "YoAccountant",
        "reporting_currency": "UGX",
        "asof": asof,
        "method": method,
        "method_label": method_label,

        "asset_nc_rows": asset_nc_rows,
        "asset_nc_total": asset_nc_total,
        "asset_curr_rows": asset_curr_rows,
        "asset_curr_total": asset_curr_total,
        "asset_total": asset_total,

        "eq_rows": eq_rows,
        "eq_total": eq_total,

        "liab_nc_rows": liab_nc_rows,
        "liab_nc_total": liab_nc_total,
        "liab_curr_rows": liab_curr_rows,
        "liab_curr_total": liab_curr_total,
        "liab_total": liab_total,

        "check_ok": check_ok,
    }

    return render(request, "balance_sheet.html", context)

# cash flow

def _parse_date_or_none(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def _balance_for_accounts(accounts_qs, date_to=None, strict_lt=False):
    """
    Returns debit-minus-credit balance for a set of accounts
    up to date_to (inclusive by default, or < date_to if strict_lt=True).
    """
    if not accounts_qs.exists():
        return 0

    lines = JournalLine.objects.filter(account__in=accounts_qs)
    if date_to:
        if strict_lt:
            lines = lines.filter(entry__date__lt=date_to)
        else:
            lines = lines.filter(entry__date__lte=date_to)

    agg = lines.aggregate(
        d=Sum("debit"),
        c=Sum("credit"),
    )
    d = agg["d"] or 0
    c = agg["c"] or 0
    return d - c  # debit-positive convention

# cashflow
def report_cashflow(request):
    """
    Statement of cash flows (very simplified, indirect method),
    with drill-down links to General Ledger for key line items.
    """

    from_str = request.GET.get("from")
    to_str = request.GET.get("to")

    dfrom = _parse_date_or_none(from_str)
    dto = _parse_date_or_none(to_str)

    today = timezone.localdate()
    if not dto:
        dto = today

    start_date = dfrom

    # Cash & bank accounts (all asset codes, but with 'cash' or 'bank')
    cash_accounts = Account.objects.filter(
        account_type__in=["CURRENT_ASSET", "NON_CURRENT_ASSET"]
    ).filter(
        Q(detail_type__icontains="cash") |
        Q(detail_type__icontains="bank")
    )

    # Accounts receivable
    ar_accounts = Account.objects.filter(
        Q(detail_type__icontains="receivable") |
        Q(account_name__icontains="receivable")
    )

    # Inventory
    inv_accounts = Account.objects.filter(
        Q(detail_type__icontains="inventory") |
        Q(account_name__icontains="inventory")
    )

    # Accounts payable
    ap_accounts = Account.objects.filter(
        Q(detail_type__icontains="payable") |
        Q(account_name__icontains="payable")
    )

    # Fixed / other non-current assets (still asset codes)
    fa_accounts = Account.objects.filter(
        account_type__in=["CURRENT_ASSET", "NON_CURRENT_ASSET"]
    ).filter(
        Q(detail_type__icontains="fixed") |
        Q(detail_type__icontains="property") |
        Q(detail_type__icontains="equipment")
    )

    # Loans – liability codes
    loan_accounts = Account.objects.filter(
        account_type__in=["CURRENT_LIABILITY", "NON_CURRENT_LIABILITY"]
    ).filter(
        Q(detail_type__icontains="loan") |
        Q(account_name__icontains="loan")
    )

    # Equity – owner’s capital / retained earnings etc.
    equity_accounts = Account.objects.filter(
        account_type="OWNER_EQUITY"
    )

    # IDs used for GL drill-down (pick first if many)
    cash_account_id = cash_accounts.first().id if cash_accounts.exists() else None
    ar_account_id = ar_accounts.first().id if ar_accounts.exists() else None
    inv_account_id = inv_accounts.first().id if inv_accounts.exists() else None
    ap_account_id = ap_accounts.first().id if ap_accounts.exists() else None
    fa_account_id = fa_accounts.first().id if fa_accounts.exists() else None
    loans_account_id = loan_accounts.first().id if loan_accounts.exists() else None
    equity_account_id = equity_accounts.first().id if equity_accounts.exists() else None

    # ---------- Net profit for the period (from GL) ----------

    period_lines = JournalLine.objects.all()
    if dfrom:
        period_lines = period_lines.filter(entry__date__gte=dfrom)
    period_lines = period_lines.filter(entry__date__lte=dto)

    # here we can still use your account_type codes:
    income_lines = period_lines.filter(
        account__account_type__in=["OPERATING_INCOME", "INVESTING_INCOME"]
    )
    expense_lines = period_lines.filter(
        account__account_type__in=[
            "OPERATING_EXPENSE", "INVESTING_EXPENSE",
            "FINANCING_EXPENSE", "INCOME_TAX_EXPENSE"
        ]
    )

    income_agg = income_lines.aggregate(d=Sum("debit"), c=Sum("credit"))
    expense_agg = expense_lines.aggregate(d=Sum("debit"), c=Sum("credit"))

    income_total = (income_agg["c"] or 0) - (income_agg["d"] or 0)      # credits - debits
    expense_total = (expense_agg["d"] or 0) - (expense_agg["c"] or 0)   # debits - credits

    net_profit = income_total - expense_total

    # ---------- Opening & closing balances for key accounts ----------

    if start_date:
        cash_start   = _balance_for_accounts(cash_accounts, start_date, strict_lt=True)
        ar_start     = _balance_for_accounts(ar_accounts, start_date, strict_lt=True)
        inv_start    = _balance_for_accounts(inv_accounts, start_date, strict_lt=True)
        ap_start     = _balance_for_accounts(ap_accounts, start_date, strict_lt=True)
        fa_start     = _balance_for_accounts(fa_accounts, start_date, strict_lt=True)
        loans_start  = _balance_for_accounts(loan_accounts, start_date, strict_lt=True)
        equity_start = _balance_for_accounts(equity_accounts, start_date, strict_lt=True)
    else:
        cash_start = ar_start = inv_start = ap_start = fa_start = loans_start = equity_start = 0

    cash_end   = _balance_for_accounts(cash_accounts, dto)
    ar_end     = _balance_for_accounts(ar_accounts, dto)
    inv_end    = _balance_for_accounts(inv_accounts, dto)
    ap_end     = _balance_for_accounts(ap_accounts, dto)
    fa_end     = _balance_for_accounts(fa_accounts, dto)
    loans_end  = _balance_for_accounts(loan_accounts, dto)
    equity_end = _balance_for_accounts(equity_accounts, dto)

    # ---------- Deltas (end - start) ----------

    delta_ar     = ar_end - ar_start
    delta_inv    = inv_end - inv_start
    delta_ap     = ap_end - ap_start
    delta_fa     = fa_end - fa_start
    delta_loans  = loans_end - loans_start
    delta_equity = equity_end - equity_start

    # ---------- Cash flows ----------

    cash_from_ops       = net_profit - delta_ar - delta_inv + delta_ap
    cash_from_investing = -delta_fa
    cash_from_financing = delta_loans + delta_equity

    net_change = cash_from_ops + cash_from_investing + cash_from_financing

    recon_ok = round(cash_start + net_change, 2) == round(cash_end, 2)

    context = {
        "company_name": "YoAccountant",
        "reporting_currency": "UGX",
        "dfrom": dfrom,
        "dto": dto,

        "net_profit": net_profit,
        "delta_ar": delta_ar,
        "delta_inv": delta_inv,
        "delta_ap": delta_ap,
        "delta_fa": delta_fa,
        "delta_loans": delta_loans,
        "delta_equity": delta_equity,

        "cash_from_ops":       cash_from_ops,
        "cash_from_investing": cash_from_investing,
        "cash_from_financing": cash_from_financing,

        "cash_start": cash_start,
        "net_change": net_change,
        "cash_end": cash_end,
        "recon_ok": recon_ok,

        "cash_account_id": cash_account_id,
        "ar_account_id": ar_account_id,
        "inv_account_id": inv_account_id,
        "ap_account_id": ap_account_id,
        "fa_account_id": fa_account_id,
        "loans_account_id": loans_account_id,
        "equity_account_id": equity_account_id,
    }

    return render(request, "cashflow.html", context)