from django.urls import reverse
from django.shortcuts import render, redirect, get_object_or_404
from django.http import HttpResponse
from openpyxl import Workbook
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from django.urls import reverse, NoReverseMatch
from django.utils import timezone
from django.utils.dateparse import parse_date
import openpyxl
import csv
import io
import os
from datetime import datetime, time, timedelta, date
from decimal import Decimal
from django.db.models import FloatField
from django.utils import timezone
from django.shortcuts import render
from django.db.models import Sum, Value, F, Q, DecimalField,ExpressionWrapper,Count 
from django.db.models.functions import Coalesce, Cast
from collections import OrderedDict
from django.core.files import File
from django.conf import settings
from django.contrib import messages
from decimal import Decimal
from django.utils import timezone
from django.db.models import Sum, F, Value, When,Case
from django.db.models import OuterRef, Subquery
from django.db.models.functions import Coalesce,Cast
from sales.views import _invoice_analytics 
from django.contrib.auth.decorators import login_required
from sales.models import Newinvoice,Payment,PaymentInvoice,SalesReceipt
from accounts.models import Account,JournalEntry,JournalLine
from sowaf.models import Newcustomer, Newsupplier
from expenses.models import Bill,Expense,Cheque
from . models import Newcustomer, Newsupplier,Newclient,Newemployee,Newasset
from accounts.utils import deposit_accounts_qs, expense_accounts_qs
from accounts.date_ranges import resolve_date_range, RANGE_LABELS, RANGE_OPTIONS
# Create your views here.
# Constants / helpers

def _as_date(d):
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return None
def status_for_invoice(inv, total: Decimal, paid: Decimal, balance: Decimal) -> str:
    """
    Simple, consistent status for an invoice.
    total/paid/balance are Decimals precomputed by the caller.
    """
    today = timezone.now().date()
    if balance <= 0:
        return "Paid"
    # overdue if due_date exists and is in the past
    due = getattr(inv, "due_date", None)
    if due and due < today:
        return "Overdue"
    if paid > 0:
        return "Partially paid"
    return "Open"


# HOME CHARTS


DEC = DecimalField(max_digits=18, decimal_places=2)
ZERO = Value(Decimal("0.00"), output_field=DEC)

# COA codes (preferred)
INCOME_TYPES = ["OPERATING_INCOME", "INVESTING_INCOME"]
EXPENSE_TYPES = ["OPERATING_EXPENSE", "INVESTING_EXPENSE", "FINANCING_EXPENSE", "INCOME_TAX_EXPENSE"]

# fallback keywords (because your P&L report uses text matching)
INCOME_KW_RE = r"income|revenue|sales"
EXPENSE_KW_RE = r"expense|expenses|cogs|cost of sales|cost of goods"


def dec(v) -> Decimal:
    """Safe Decimal converter."""
    try:
        return Decimal(str(v or "0"))
    except Exception:
        return Decimal("0.00")


def bankish_q():
    return (
        Q(detail_type__icontains="bank") |
        Q(detail_type__icontains="cash") |
        Q(detail_type__icontains="cash and cash equivalents") |
        Q(detail_type__icontains="cash on hand")
    )



def _income_jl_filter():
    return (
        Q(account__account_type__in=INCOME_TYPES) |
        Q(account__parent__account_type__in=INCOME_TYPES) |
        Q(account__account_name__iregex=INCOME_KW_RE) |
        Q(account__detail_type__iregex=INCOME_KW_RE) |
        Q(account__parent__account_name__iregex=INCOME_KW_RE) |
        Q(account__parent__detail_type__iregex=INCOME_KW_RE)
    )


def _expense_jl_filter():
    return (
        Q(account__account_type__in=EXPENSE_TYPES) |
        Q(account__parent__account_type__in=EXPENSE_TYPES) |
        Q(account__account_name__iregex=EXPENSE_KW_RE) |
        Q(account__detail_type__iregex=EXPENSE_KW_RE) |
        Q(account__parent__account_name__iregex=EXPENSE_KW_RE) |
        Q(account__parent__detail_type__iregex=EXPENSE_KW_RE)
    )
def _to_decimal_number(x) -> Decimal:
    """
    Convert DB numeric (float/int/Decimal/None) safely to Decimal.
    Using str(...) avoids float binary issues more than Decimal(float).
    """
    if x is None:
        return Decimal("0")
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


# ===========================
# HOME VIEW
# ===========================

def home(request):
    today = timezone.localdate()

    # QB-style dropdowns per tile (like QuickBooks)
    pnl_range_key   = request.GET.get("pnl_range", "last_month")
    exp_range_key   = request.GET.get("exp_range", "last_30_days")
    sales_range_key = request.GET.get("sales_range", "this_financial_year_to_date")

    # invoices range key (default "as of today" feel)
    inv_range_key = request.GET.get("inv_range", "this_month_to_date")

    pnl_from, pnl_to     = resolve_date_range(pnl_range_key)
    exp_from, exp_to     = resolve_date_range(exp_range_key)
    sales_from, sales_to = resolve_date_range(sales_range_key)
    inv_from, inv_to     = resolve_date_range(inv_range_key)

    pnl_label   = RANGE_LABELS.get(pnl_range_key, "Last month")
    exp_label   = RANGE_LABELS.get(exp_range_key, "Last 30 days")
    sales_label = RANGE_LABELS.get(sales_range_key, "This financial year to date")
    inv_label   = RANGE_LABELS.get(inv_range_key, "This month to date")

    # =========================================================
    # 1) PROFIT & LOSS TILE (selected range)
    # =========================================================
    jl_pnl = (
        JournalLine.objects
        .select_related("entry", "account", "account__parent")
        .filter(entry__date__gte=pnl_from, entry__date__lte=pnl_to)
    )

    income_lines  = jl_pnl.filter(_income_jl_filter())
    expense_lines = jl_pnl.filter(_expense_jl_filter())

    inc_agg = income_lines.aggregate(
        deb=Coalesce(Sum("debit"), ZERO, output_field=DEC),
        cre=Coalesce(Sum("credit"), ZERO, output_field=DEC),
    )
    exp_agg = expense_lines.aggregate(
        deb=Coalesce(Sum("debit"), ZERO, output_field=DEC),
        cre=Coalesce(Sum("credit"), ZERO, output_field=DEC),
    )

    income_amt  = dec(inc_agg["cre"]) - dec(inc_agg["deb"])      # credit - debit
    expense_amt = dec(exp_agg["deb"]) - dec(exp_agg["cre"])      # debit - credit
    net_profit  = income_amt - expense_amt

    pnl_tile = {
        "range_label": pnl_label,
        "net_profit": net_profit,
        "income": income_amt,
        "expense": expense_amt,
        "pnl_url": "accounts:report-pnl",
        "pnl_params": f"?from={pnl_from:%Y-%m-%d}&to={pnl_to:%Y-%m-%d}",
    }

    # =========================================================
    # 2) EXPENSES TILE (donut, selected range)
    # =========================================================
    jl_exp = (
        JournalLine.objects
        .select_related("entry", "account", "account__parent")
        .filter(entry__date__gte=exp_from, entry__date__lte=exp_to)
        .filter(_expense_jl_filter())
        .values("account__account_name")
        .annotate(
            deb=Coalesce(Sum("debit"), ZERO, output_field=DEC),
            cre=Coalesce(Sum("credit"), ZERO, output_field=DEC),
        )
        .order_by("account__account_name")
    )

    exp_labels     = [r["account__account_name"] or "Expense" for r in jl_exp]
    exp_values_dec = [(dec(r["deb"]) - dec(r["cre"])) for r in jl_exp]
    exp_values     = [float(v) for v in exp_values_dec]
    exp_total      = sum(exp_values_dec, Decimal("0.00"))

    expenses_tile = {
        "range_label": exp_label,
        "total": exp_total,
        "spending_url": "expenses:expenses",
    }

    # =========================================================
    # 3) BANK ACCOUNTS TILE (as of today)
    # =========================================================
    bank_accounts = (
        Account.objects
        .filter(is_active=True)
        .filter(bankish_q())
        .order_by("account_name")
    )

    bank_rows = []
    for acc in bank_accounts:
        agg = acc.journalline_set.filter(entry__date__lte=today).aggregate(
            deb=Coalesce(Sum("debit"), ZERO, output_field=DEC),
            cre=Coalesce(Sum("credit"), ZERO, output_field=DEC),
        )
        bal = dec(agg["deb"]) - dec(agg["cre"])
        bank_rows.append({
            "id": acc.id,
            "name": acc.account_name or "Bank",
            "balance": bal,
            "gl_url": "accounts:general-ledger",
            "gl_params": f"?account_id={acc.id}&to={today:%Y-%m-%d}",
        })

    bs_url = "accounts:report-balance-sheet"

    # =========================================================
    # 4) INVOICES TILE (✅ FIXED: no __date, SQLite-safe range)
    # =========================================================
    tz = timezone.get_current_timezone()

    inv_from_dt = timezone.make_aware(datetime.combine(inv_from, time.min), tz)
    inv_to_dt   = timezone.make_aware(datetime.combine(inv_to + timedelta(days=1), time.min), tz)  # exclusive end

    inv_qs = (
        Newinvoice.objects
        .filter(date_created__gte=inv_from_dt, date_created__lt=inv_to_dt)
        .annotate(
            total_due_f=Coalesce(Cast("total_due", FloatField()), Value(0.0), output_field=FloatField()),
            total_paid_f=Coalesce(
                Sum(Cast("payments_applied__amount_paid", FloatField())),
                Value(0.0),
                output_field=FloatField(),
            ),
        )
        .order_by("-date_created", "-id")
    )

    overdue_amount = Decimal("0.00")
    unpaid_amount  = Decimal("0.00")
    paid_amount    = Decimal("0.00")

    overdue_count = 0
    unpaid_count  = 0
    paid_count    = 0

    for inv in inv_qs:
        total = _to_decimal_number(inv.total_due_f)
        paid  = _to_decimal_number(inv.total_paid_f)
        bal   = total - paid

        if bal <= Decimal("0.00001"):
            paid_count += 1
            paid_amount += total
        else:
            due = _as_date(getattr(inv, "due_date", None))
            if due and due < today:
                overdue_count += 1
                overdue_amount += bal
            else:
                unpaid_count += 1
                unpaid_amount += bal

    invoices_tile = {
        "range_label": inv_label,
        "overdue_amount": overdue_amount,
        "not_due_amount": unpaid_amount,
        "paid_30_amount": paid_amount,
        "overdue_count": overdue_count,
        "not_due_count": unpaid_count,
        "paid_count": paid_count,
        "invoices_url": "sales:invoices",
    }

    # =========================================================
    # 5) SALES TILE (✅ FIXED: no __date, same logic)
    # =========================================================
    sales_from_dt = timezone.make_aware(datetime.combine(sales_from, time.min), tz)
    sales_to_dt   = timezone.make_aware(datetime.combine(sales_to + timedelta(days=1), time.min), tz)  # exclusive end

    inv_rows = (
        Newinvoice.objects
        .filter(date_created__gte=sales_from_dt, date_created__lt=sales_to_dt)
        .values_list("date_created", "total_due")
        .order_by("date_created")
    )

    months = OrderedDict()
    for created_dt, total_due in inv_rows:
        if not created_dt:
            continue
        key = created_dt.strftime("%b")
        months.setdefault(key, Decimal("0.00"))
        months[key] += dec(total_due)

    sales_tile = {
        "range_label": sales_label,
        "total": sum(months.values(), Decimal("0.00")),
        "labels": list(months.keys()),
        "values": [float(v) for v in months.values()],
    }

    context = {
        "pnl_tile": pnl_tile,
        "expenses_tile": expenses_tile,
        "bank_rows": bank_rows,
        "bs_url": bs_url,
        "invoices_tile": invoices_tile,
        "sales_tile": sales_tile,

        "exp_labels": exp_labels,
        "exp_values": exp_values,

        "range_options": RANGE_OPTIONS,
        "pnl_range_key": pnl_range_key,
        "exp_range_key": exp_range_key,
        "sales_range_key": sales_range_key,
        "inv_range_key": inv_range_key,
    }

    return render(request, "Home.html", context)
  # assets view
def assets(request):
    assets = Newasset.objects.all()
      
    return render(request, 'Assets.html', {'assets':assets})
# assets form 
def add_assests(request):
    if request.method=='POST':
            # getting the supplier by id since its a foreign key
        supplier_id = request.POST.get('supplier')
        supplier=None
        if supplier_id:
            try:
                supplier = Newsupplier.objects.get(pk=supplier_id)
            except Newsupplier.DoesNotExist:
                supplier=None
        
        
        
        asset_name = request.POST.get('asset_name')
        asset_tag = request.POST.get('asset_tag')
        asset_category = request.POST.get('asset_category')
        asset_description = request.POST.get('asset_description')
        department = request.POST.get('department')
        custodian = request.POST.get('custodian')
        asset_status = request.POST.get('asset_status')
        purchase_price = request.POST.get('purchase_price')

        funding_source = request.POST.get('funding_source')
        life_span = request.POST.get('life_span')
        depreciation_method = request.POST.get('depreciation_method')
        residual_value = request.POST.get('residual_value')
        accumulated_depreciation = request.POST.get('accumulated_depreciation')
        remaining_value = request.POST.get('remaining_value')
        asset_account = request.POST.get('asset_account')
        capitalization_date = request.POST.get('capitalization_date')
        cost_center = request.POST.get('cost_center')
        asset_condition = request.POST.get('asset_condition')
        maintenance_schedule = request.POST.get('maintenance_schedule')
        insurance_details = request.POST.get('insurance_details')
        notes = request.POST.get('notes')
        asset_attachments =request.FILES.get('asset_attachments')
# handling the date 
        capitalization_date_str = request.POST.get('capitalization_date')
        capitalization_date = None
        if capitalization_date_str:
            try:
                capitalization_date = datetime.strptime(capitalization_date_str, '%d/%m/%Y')
            except ValueError:
                capitalization_date = None  # Or handle error
# purchase date
        purchase_date_str = request.POST.get('purchase_date')
        purchase_date = None
        if purchase_date_str:
            try:
                purchase_date = datetime.strptime(purchase_date_str, '%d/%m/%Y')
            except ValueError:
                purchase_date = None 

    # waranty date
        warranty_str = request.POST.get('warranty')
        warranty = None
        if warranty_str:
            try:
                warranty = datetime.strptime(warranty_str, '%d/%m/%Y')
            except ValueError:
                warranty = None 

    # saving the assets
        asset = Newasset(asset_name=asset_name,asset_tag=asset_tag,asset_category=asset_category,asset_description=asset_description,department=department,custodian=custodian,asset_status=asset_status,purchase_price=purchase_price,purchase_date=purchase_date,supplier=supplier,warranty=warranty,funding_source=funding_source,life_span=life_span,depreciation_method=depreciation_method,residual_value=residual_value,accumulated_depreciation=accumulated_depreciation,remaining_value=remaining_value,asset_account=asset_account,capitalization_date=capitalization_date,cost_center=cost_center,asset_condition=asset_condition,maintenance_schedule=maintenance_schedule,insurance_details=insurance_details,notes=notes,asset_attachments=asset_attachments,)

        asset.save()
        # adding button save actions
        save_action = request.POST.get('save_action')
        if save_action == 'save&new':
            return redirect('add-asset')
        elif save_action == 'save&close':
            return redirect('assets')
    suppliers = Newsupplier.objects.all()
    return render(request, 'assets_form.html', {'suppliers':suppliers})
# editing assets
def edit_asset(request, pk):
    asset = get_object_or_404(Newasset,pk=pk)
    if request.method=='POST':
        asset.asset_name = request.POST.get('asset_name',asset.asset_name)
        asset.asset_tag = request.POST.get('asset_tag',asset.asset_tag)
        asset.asset_category = request.POST.get('asset_category',asset.asset_category)
        asset.asset_description = request.POST.get('asset_description',asset.asset_description)
        asset.department = request.POST.get('department',asset.department)
        asset.custodian = request.POST.get('custodian',asset.custodian)
        asset.asset_status = request.POST.get('asset_status',asset.asset_status)
        asset.purchase_price = request.POST.get('purchase_price',asset.purchase_price)
        asset.purchase_date = request.POST.get('purchase_date',asset.purchase_date)

        asset.funding_source = request.POST.get('funding_source',asset.funding_source)
        asset.life_span = request.POST.get('life_span',asset.life_span) 
        asset.depreciation_method = request.POST.get('depreciation_method',asset.depreciation_method)
        asset.residual_value = request.POST.get('residual_value',asset.residual_value)
        asset.accumulated_depreciation = request.POST.get('accumulated_depreciation',asset.accumulated_depreciation)
        asset.remaining_value = request.POST.get('remaining_value',asset.remaining_value)
        asset.asset_account = request.POST.get('asset_account',asset.asset_account)
        asset.cost_center = request.POST.get('cost_center',asset.cost_center)
        asset.asset_condition = request.POST.get('asset_condition',asset.asset_condition)
        asset.maintenance_schedule = request.POST.get('maintenance_schedule',asset.maintenance_schedule)
        asset.insurance_details = request.POST.get('insurance_details',asset.insurance_details)
        asset.notes = request.POST.get('notes',asset.notes)
        
        # Handle ForeignKey (supplier)
        supplier_id = request.POST.get('supplier')
        if supplier_id:
            try:
                asset.supplier = Newsupplier.objects.get(pk=supplier_id)
            except Newsupplier.DoesNotExist:
                asset.supplier = None
        # handling the date 
        capitalization_date_str = request.POST.get('capitalization_date')
        if capitalization_date_str:
            
            try:
                asset.capitalization_date = datetime.strptime(capitalization_date_str, '%d/%m/%Y')
            except ValueError:
                pass  # Keep the original value or handle error

        purchase_date_str = request.POST.get('purchase_date')
        if purchase_date_str:
            try:
                asset.purchase_date = datetime.strptime(purchase_date_str, '%d/%m/%Y')
            except ValueError:
                pass  # Keep the original value or handle error

        warranty_str = request.POST.get('warranty')
        if warranty_str:
            try:
                asset.warranty = datetime.strptime(warranty_str, '%d/%m/%Y')
            except ValueError:
                pass  
                
# working on the files
        if 'asset_attachments' in request.FILES:
            asset.asset_attachments = request.FILES['asset_attachments']
        asset.save()

        return redirect('sowaf:assets')
    suppliers = Newsupplier.objects.all()
    return render(request, 'assets_form.html', {'asset': asset,'suppliers': suppliers})
# deleting an asset
def delete_asset(request, pk):
    customer = get_object_or_404(Newasset, pk=pk)
    customer.delete()
    return redirect('sowaf:assets')

# importing assets
def download_assets_template(request):
    wb = Workbook()
    ws = wb.active
    ws.title = "Assets Template"

    headers = [
        'asset_name','asset_tag','asset_category','asset_description','department','custodian','asset_status','purchase_price','purchase_date','supplier','warranty','funding_source','life_span','depreciation_method','residual_value','accumulated_depreciation','remaining_value','asset_account','capitalization_date','cost_center','asset_condition','maintenance_schedule','insurance_details','notes',
    ]
    ws.append(headers)

    with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        wb.save(tmp.name)
        tmp.seek(0)
        response = HttpResponse(tmp.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="assets_template.xlsx"'
        return response
# functions to handle the date formats
# Parse capitalization_date (multiple formats)
def parse_capitalization_date_safe(capitalization_date):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(capitalization_date), fmt).date()
        except (ValueError, TypeError):
            continue
    return None

# Parse purchase_date (multiple formats)
def parse_purchase_date_safe(purchase_date):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(purchase_date), fmt).date()
        except (ValueError, TypeError):
            continue
    return None
# Parse warranty (multiple formats)
def parse_warranty_safe(warranty):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(warranty), fmt).date()
        except (ValueError, TypeError):
            continue
    return None
# actual import
def import_assets(request):
    if request.method != 'POST' or 'excel_file' not in request.FILES:
        messages.error(request, "No file uploaded.")
        return redirect('sowaf:assets')

    excel_file = request.FILES['excel_file']
    file_name = excel_file.name.lower()

    try:
        if file_name.endswith('.csv'):
            decoded_file = excel_file.read().decode('utf-8')
            io_string = io.StringIO(decoded_file)
            reader = csv.reader(io_string)
            next(reader)  
            for row in reader:
                (
                    asset_name,asset_tag,asset_category,asset_description,department,custodian,asset_status,purchase_price,purchase_date,supplier,warranty,funding_source,life_span,depreciation_method,residual_value,accumulated_depreciation,remaining_value,asset_account,capitalization_date,cost_center,asset_condition,maintenance_schedule,insurance_details,notes,
                ) = row
                capitalization_date = parse_capitalization_date_safe(capitalization_date)
                purchase_date = parse_purchase_date_safe(purchase_date)
                warranty = parse_warranty_safe(warranty)
                
                asset = Newasset.objects.create(
                    
                    asset_name=asset_name,
                    asset_tag=asset_tag,
                    asset_category=asset_category,
                    asset_description=asset_description,
                    department=department,
                    custodian=custodian,
                    asset_status=asset_status,
                    purchase_price=purchase_price,
                    purchase_date=purchase_date,
                    supplier=supplier,
                    warranty=warranty,
                    funding_source=funding_source,
                    life_span=life_span,
                    depreciation_method=depreciation_method,
                    residual_value=residual_value,
                    accumulated_depreciation=accumulated_depreciation,
                    remaining_value=remaining_value,
                    asset_account=asset_account,
                    capitalization_date=capitalization_date,
                    cost_center=cost_center,
                    asset_condition=asset_condition,
                    maintenance_schedule=maintenance_schedule,
                    insurance_details=insurance_details,
                    notes=notes,
                )

        elif file_name.endswith('.xlsx'):
            wb = openpyxl.load_workbook(excel_file)
            sheet = wb.active

            for row in sheet.iter_rows(min_row=2, values_only=True):
                (
                asset_name,asset_tag,asset_category,asset_description,department,custodian,asset_status,purchase_price,purchase_date,supplier,warranty,funding_source,life_span,depreciation_method,residual_value,accumulated_depreciation,remaining_value,asset_account,capitalization_date,cost_center,asset_condition,maintenance_schedule,insurance_details,notes,
                ) = row

                capitalization_date = parse_capitalization_date_safe(capitalization_date)
                purchase_date = parse_purchase_date_safe(purchase_date)
                warranty = parse_warranty_safe(warranty)

                asset = Newasset.objects.create(
                    asset_name=asset_name,
                    asset_tag=asset_tag,
                    asset_category=asset_category,
                    asset_description=asset_description,
                    department=department,
                    custodian=custodian,
                    asset_status=asset_status,
                    purchase_price=purchase_price,
                    purchase_date=purchase_date,
                    supplier=supplier,
                    warranty=warranty,
                    funding_source=funding_source,
                    life_span=life_span,
                    depreciation_method=depreciation_method,
                    residual_value=residual_value,
                    accumulated_depreciation=accumulated_depreciation,
                    remaining_value=remaining_value,
                    asset_account=asset_account,
                    capitalization_date=capitalization_date,
                    cost_center=cost_center,
                    asset_condition=asset_condition,
                    maintenance_schedule=maintenance_schedule,
                    insurance_details=insurance_details,
                    notes=notes,
                )
        else:
            messages.error(request, "Unsupported file type. Please upload a .csv or .xlsx file.")
            return redirect('sowaf:assets')

        messages.success(request, "asset data imported successfully.")
        return redirect('sowaf:assets')

    except Exception as e:
        messages.error(request, f"Import failed: {str(e)}")
        return redirect('sowaf:assets')

# customer view
def _dec(x):
    try:
        return Decimal(str(x or "0"))
    except Exception:
        return ZERO


def customers(request):
    q = (request.GET.get("q") or "").strip()

    # ---------- Correlated subqueries ----------
    # total paid per invoice (scalar subquery)
    paid_per_invoice_sq = (
        PaymentInvoice.objects
        .filter(invoice_id=OuterRef("pk"))
        .values("invoice_id")
        .annotate(total=Coalesce(Sum("amount_paid"), Value(Decimal("0.00"))))
        .values("total")[:1]
    )

    # invoices annotated with due (Decimal), paid (Subquery), outstanding (ExpressionWrapper)
    inv_all = (
        Newinvoice.objects
        .annotate(
            total_due_dec=Cast(F("total_due"), DecimalField(max_digits=18, decimal_places=2)),
            paid=Coalesce(Subquery(paid_per_invoice_sq, output_field=DecimalField(max_digits=18, decimal_places=2)),
                          Value(Decimal("0.00")))
        )
        .annotate(
            raw_outstanding=ExpressionWrapper(F("total_due_dec") - F("paid"),
                                              output_field=DecimalField(max_digits=18, decimal_places=2)),
            outstanding=Case(
                When(raw_outstanding__gt=0, then=F("raw_outstanding")),
                default=Value(Decimal("0.00")),
                output_field=DecimalField(max_digits=18, decimal_places=2),
            )
        )
    )

    # per-customer open balance (sum of outstanding for that customer) as Subquery
    per_customer_open_sq = (
        inv_all.filter(customer_id=OuterRef("pk"))
        .values("customer_id")
        .annotate(sum_outstanding=Coalesce(Sum("outstanding"), Value(Decimal("0.00"))))
        .values("sum_outstanding")[:1]
    )

    # ---------- Base customers queryset with open_balance annotation ----------
    customers_qs = (
        Newcustomer.objects
        .annotate(
            open_balance=Coalesce(
                Subquery(per_customer_open_sq, output_field=DecimalField(max_digits=18, decimal_places=2)),
                Value(Decimal("0.00"))
            )
        )
    )
    if q:
        customers_qs = customers_qs.filter(
            Q(customer_name__icontains=q) | Q(company_name__icontains=q)
        )

    # ---------- Dashboard analytics (all computed from inv_all; no nested aggregates) ----------
    today = timezone.now().date()
    cutoff = today - timedelta(days=30)

    # open = sum of outstanding where > 0
    open_agg = inv_all.filter(outstanding__gt=0).aggregate(
        amount=Coalesce(Sum("outstanding"), Value(Decimal("0.00"))),
        count=Count("id")
    )

    # overdue = outstanding where due_date < today
    overdue_agg = inv_all.filter(outstanding__gt=0, due_date__lt=today).aggregate(
        amount=Coalesce(Sum("outstanding"), Value(Decimal("0.00"))),
        count=Count("id")
    )

    # unbilled (if you don’t track estimates/drafts, keep zeros)
    unbilled_amount = Decimal("0.00")
    unbilled_count = 0

    # recent payments = (applied payments in last 30d) + (cash sales receipts paid in last 30d)
    recent_paid_from_payments = (
        PaymentInvoice.objects
        .filter(payment__payment_date__gte=cutoff)
        .aggregate(total=Coalesce(Sum("amount_paid"), Value(Decimal("0.00"))))
        ["total"]
    ) or Decimal("0.00")

    recent_paid_from_receipts = (
        SalesReceipt.objects
        .filter(receipt_date__gte=cutoff)
        .aggregate(total=Coalesce(Sum("amount_paid"), Value(Decimal("0.00"))))
        ["total"]
    ) or Decimal("0.00")

    amount_recent = recent_paid_from_payments + recent_paid_from_receipts
    recent_payments_count = Payment.objects.filter(payment_date__gte=cutoff).count()
    recent_receipts_count = SalesReceipt.objects.filter(receipt_date__gte=cutoff).count()
    count_recent = recent_payments_count + recent_receipts_count

    total_for_pct = (
        (unbilled_amount or Decimal("0")) +
        (overdue_agg["amount"] or Decimal("0")) +
        (open_agg["amount"] or Decimal("0")) +
        (amount_recent or Decimal("0"))
    ) or Decimal("1")

    def pct(x):
        return float((x or Decimal("0")) * Decimal("100") / total_for_pct)

    analytics = {
        "amount_unbilled": unbilled_amount,
        "count_unbilled": unbilled_count,

        "amount_overdue": overdue_agg["amount"] or Decimal("0"),
        "count_overdue": int(overdue_agg["count"] or 0),

        "amount_open": open_agg["amount"] or Decimal("0"),
        "count_open": int(open_agg["count"] or 0),

        "amount_recent": amount_recent,
        "count_recent": count_recent,

        "pct_unbilled": pct(unbilled_amount),
        "pct_overdue": pct(overdue_agg["amount"]),
        "pct_open": pct(open_agg["amount"]),
        "pct_recent": pct(amount_recent),
    }

    return render(request, "Customers.html", {
        "customers": customers_qs,
        "q": q,
        "analytics": analytics,
    })

def _cast(field_name):
    return Cast(F(field_name), DEC)
def _safe_url(pattern_name, *args):
    try:
        return reverse(pattern_name, args=args)
    except NoReverseMatch:
        return "#"

def customer_detail(request, pk):
    customer = get_object_or_404(Newcustomer, pk=pk)
    tab = request.GET.get("tab", "transactions")

    inv_qs_base = (
        Newinvoice.objects
        .filter(customer=customer)
        .annotate(
            total_due_dec=Cast("total_due", DecimalField(max_digits=18, decimal_places=2)),
            total_paid=Coalesce(Sum("payments_applied__amount_paid"), Value(Decimal("0.00")))
        )
    )

    # Open/Overdue balances (as you had)
    from django.utils.timezone import now
    today = now().date()
    open_balance = sum(
        (row.total_due_dec or Decimal("0.00")) - (row.total_paid or Decimal("0.00"))
        for row in inv_qs_base
    )
    overdue_balance = sum(
        (row.total_due_dec or Decimal("0.00")) - (row.total_paid or Decimal("0.00"))
        for row in inv_qs_base.filter(due_date__lt=today)
    )
    count_invoices = inv_qs_base.count()

    # ---------- transactions (existing code, unchanged) ----------
    transactions_rows = []
    inv_qs = inv_qs_base.select_related("customer").order_by("-date_created", "-id")

    def _fallback_status(inv, total, paid, bal):
        if bal <= 0:
            return "Paid"
        if getattr(inv, "due_date", None) and inv.due_date < today:
            return "Overdue"
        return "Open"

    for inv in inv_qs:
        total = inv.total_due_dec or Decimal("0.00")
        paid  = inv.total_paid or Decimal("0.00")
        bal   = max(total - paid, Decimal("0.00"))
        try:
            status = status_for_invoice(inv, total, paid, bal)
        except NameError:
            status = _fallback_status(inv, total, paid, bal)

        transactions_rows.append({
            "id": inv.id,
            "date": getattr(inv, "date_created", None),
            "type": "Invoice",
            "no": f"INV-{inv.id:04d}",
            "customer": customer.customer_name,
            "memo": (inv.memo or "")[:140],
            "amount": total,
            "status": status,
            "edit_url":  reverse("sales:edit-invoice", args=[inv.id]),
            "view_url":  reverse("sales:invoice-detail", args=[inv.id]),
            "print_url": reverse("sales:invoice-print", args=[inv.id]),
        })

    pay_qs = (
        Payment.objects
        .filter(customer=customer)
        .annotate(applied_total=Coalesce(Sum("applied_invoices__amount_paid"), Value(Decimal("0.00"))))
        .order_by("-payment_date", "-id")
    )
    for p in pay_qs:
        transactions_rows.append({
            "id": p.id,
            "date": p.payment_date,
            "type": "Payment",
            "no": (p.reference_no or f"{p.id:04d}"),
            "customer": customer.customer_name,
            "memo": (p.memo or "")[:140],
            "amount": p.applied_total or Decimal("0.00"),
            "status": "Closed" if (p.applied_total or 0) > 0 else "Unapplied",
            "edit_url":  reverse("sales:payment-edit", args=[p.id]),
            "view_url":  reverse("sales:payment-detail", args=[p.id]),
            "print_url": reverse("sales:payment-print", args=[p.id]),
        })

    try:
        sr_qs = SalesReceipt.objects.filter(customer=customer).order_by("-receipt_date", "-id")
        for r in sr_qs:
            amount = Decimal(str(r.total_amount or "0"))
            transactions_rows.append({
                "id": r.id,
                "date": getattr(r, "receipt_date", None),
                "type": "Sales Receipt",
                "no": (r.reference_no or f"SR-{r.id:04d}"),
                "customer": customer.customer_name,
                "memo": (r.memo or "")[:140],
                "amount": amount,
                "status": "Closed",
                "edit_url":  _safe_url("sales:receipt-edit", r.id),
                "view_url":  _safe_url("sales:receipt-detail", r.id),
                "print_url": _safe_url("sales:receipt-print", r.id),
            })
    except NameError:
        pass

    transactions_rows.sort(key=lambda r: (r["date"] or today, r["type"], r["id"]), reverse=True)

    # ---------- NEW: statements for this customer ----------
    statements_rows = []
    try:
        from sales.models import Statement  # if not already imported
        st_qs = Statement.objects.filter(customer=customer).order_by("-statement_date", "-id")
        for st in st_qs:
            # use get_*_display when choices exist
            st_type = getattr(st, "get_statement_type_display", None)
            st_type = st_type() if callable(st_type) else st.statement_type

            statements_rows.append({
                "id": st.id,
                "date": st.statement_date,
                "no": f"ST-{st.id:04d}",
                "type": st_type,
                "start": st.start_date,
                "end":   st.end_date,
                "balance": st.closing_balance or Decimal("0.00"),
                "view_url":  _safe_url("sales:statement-detail", st.id),
                "print_url": _safe_url("sales:statement-print", st.id),
                "send_url":  _safe_url("sales:statement-send", st.id),
            })
    except Exception:
        # If the Statement model or urls aren’t present yet, just keep the tab empty
        statements_rows = []

    context = {
        "customer": customer,
        "tab": tab,
        "open_balance": open_balance,
        "overdue_balance": overdue_balance,
        "count_invoices": count_invoices,
        "transactions_rows": transactions_rows,
        "statements_rows": statements_rows,   # <-- pass to template
    }
    return render(request, "customer_detail.html", context)
# making a customer active and inactive
def make_inactive_customer(request, pk):
    customer = get_object_or_404(Newcustomer, pk=pk)
    customer.is_active = False
    customer.save()
    return redirect('sowaf:customers')
# reactivating
def make_active_customer(request, pk):
    customer = get_object_or_404(Newcustomer, pk=pk)
    customer.is_active = True
    customer.save()
    return redirect('sowaf:customers')

# customer form view

def add_customer(request):
    if request.method == 'POST':
        logo =request.FILES.get('logo')
        if logo:
            if not logo.name.lower().endswith('.png'):
                messages.error(request, "Only PNG files are allowed for the logo.")
                return redirect(request.path)
            # restricting the photo size
            if logo.size > 1048576:
                messages.error(request, "logo file size must not exceed 800kps.")
                return redirect(request.path)
        customer_name =request.POST.get('name')
        company_name =request.POST.get('company')
        email =request.POST.get('email')
        phone_number =request.POST.get('phonenum')
        mobile_number =request.POST.get('mobilenum')
        website =request.POST.get('website')
        tin_number =request.POST.get('tin')
        raw_balance = request.POST.get('balance')
        try:
            opening_balance = Decimal(raw_balance) if raw_balance else Decimal("0.00")
        except:
            opening_balance = Decimal("0.00")

        registration_date = request.POST.get('registration_date')                
        street_one =request.POST.get('street1')
        street_two =request.POST.get('street2')
        city =request.POST.get('city')
        province =request.POST.get('province')
        postal_code =request.POST.get('postalcode')
        country =request.POST.get('country')
        notes =request.POST.get('notes')
        attachments =request.FILES.get('attachments')
        new_customer = Newcustomer(logo=logo,customer_name=customer_name,company_name=company_name,email=email,phone_number=phone_number,mobile_number=mobile_number,website=website,tin_number=tin_number,opening_balance=opening_balance,registration_date=registration_date,street_one=street_one,street_two=street_two,city=city,province=province,postal_code=postal_code,country=country,notes=notes,attachments=attachments)
        new_customer.save()
        if opening_balance != 0:

            #1. Get Accounts Receivable
            ar_account = Account.objects.filter(account_name="Accounts Receivable").first()

            if not ar_account:
                messages.error(request, "Accounts Receivable account is missing in Chart of Accounts.")
                return redirect(request.path)

            #2. Get Opening Balance Equity
            opening_equity_acct, _ = Account.objects.get_or_create(
                account_name="Opening Balance Equity",
                account_type="OWNER_EQUITY",
                defaults={
                    "detail_type": "Opening balances",
                    "is_active": True,
                },
            )

            #3. Create Journal Entry
            je = JournalEntry.objects.create(
                date=registration_date,
                description=f"Opening balance for customer {new_customer.customer_name}",
                source_type="CUSTOMER_OPENING_BALANCE",
                source_id=new_customer.id,
            )

            amount = abs(opening_balance)

            #4. Post Journal Lines
            # DR Accounts Receivable
            JournalLine.objects.create(
                entry=je,
                account=ar_account,
                debit=amount,
                credit=Decimal("0"),
            )

            # CR Opening Balance Equity
            JournalLine.objects.create(
                entry=je,
                account=opening_equity_acct,
                debit=Decimal("0"),
                credit=amount,
            )


        # adding save actions
        save_action = request.POST.get('save_action')
        if save_action == 'save':
            return redirect('sowaf:customers')
        if save_action == 'save&new':
            return redirect('sowaf:add-customer')
        elif save_action == 'save&close':
            return redirect('sowaf:customers')
       
    return render(request, 'customers_form.html', {})
# editing the customer table
from decimal import Decimal
from django.db import transaction
from django.utils import timezone
from accounts.models import Account, JournalEntry, JournalLine

@transaction.atomic
def edit_customer(request, pk):
    customer = get_object_or_404(Newcustomer, pk=pk)

    if request.method == 'POST':

        # ---------- BASIC FIELDS ----------
        customer.customer_name = request.POST.get('name', customer.customer_name)
        customer.company_name  = request.POST.get('company', customer.company_name)
        customer.email         = request.POST.get('email', customer.email)
        customer.phone_number  = request.POST.get('phonenum', customer.phone_number)
        customer.mobile_number = request.POST.get('mobilenum', customer.mobile_number)
        customer.website       = request.POST.get('website', customer.website)
        customer.tin_number    = request.POST.get('tin', customer.tin_number)

        # ---------- OPENING BALANCE (SAFE DECIMAL) ----------
        raw_balance = request.POST.get('balance')
        try:
            new_opening_balance = Decimal(raw_balance) if raw_balance else Decimal("0.00")
        except:
            new_opening_balance = Decimal("0.00")

        customer.opening_balance = new_opening_balance

        # ---------- DATE ----------
        customer.registration_date = request.POST.get('registration_date',customer.registration_date)    

        # ---------- ADDRESS ----------
        customer.street_one  = request.POST.get('street1', customer.street_one)
        customer.street_two  = request.POST.get('street2', customer.street_two)
        customer.city        = request.POST.get('city', customer.city)
        customer.province    = request.POST.get('province', customer.province)
        customer.postal_code = request.POST.get('postalcode', customer.postal_code)
        customer.country     = request.POST.get('country', customer.country)
        customer.notes       = request.POST.get('notes', customer.notes)

        # ---------- LOGO ----------
        logo = request.FILES.get('logo')
        if logo:
            if not logo.name.lower().endswith('.png'):
                messages.error(request, "Only PNG files are allowed for the logo.")
                return redirect(request.path)

            if logo.size > 1048576:
                messages.error(request, "Logo file size must not exceed 1MB.")
                return redirect(request.path)

            customer.logo = logo

        # ---------- ATTACHMENTS ----------
        if 'attachments' in request.FILES:
            customer.attachments = request.FILES['attachments']

        customer.save()

        # =========================================================
        #UPDATE CUSTOMER OPENING BALANCE JOURNAL ENTRY
        # =========================================================

        je = JournalEntry.objects.filter(
            source_type="CUSTOMER_OPENING_BALANCE",
            source_id=customer.id
        ).first()

        #If balance is zero → remove any existing JE
        if new_opening_balance == 0:
            if je:
                je.delete()

        else:
            #Get Accounts Receivable
            ar_account = Account.objects.filter(account_name="Accounts Receivable").first()
            if not ar_account:
                messages.error(request, "Accounts Receivable account is missing in Chart of Accounts.")
                return redirect(request.path)

            #Get Opening Balance Equity
            opening_equity_acct, _ = Account.objects.get_or_create(
                account_name="Opening Balance Equity",
                account_type="OWNER_EQUITY",
                defaults={
                    "detail_type": "Opening balances",
                    "is_active": True,
                },
            )

            amount = abs(new_opening_balance)

            #Create or Update Journal Entry
            if not je:
                je = JournalEntry.objects.create(
                    date=customer.registration_date or timezone.now().date(),
                    description=f"Opening balance for customer {customer.customer_name}",
                    source_type="CUSTOMER_OPENING_BALANCE",
                    source_id=customer.id,
                )
            else:
                je.date = customer.registration_date or timezone.now().date()
                je.description = f"Opening balance for customer {customer.customer_name}"
                je.save()

                #Clear old lines
                JournalLine.objects.filter(entry=je).delete()

            #DR Accounts Receivable
            JournalLine.objects.create(
                entry=je,
                account=ar_account,
                debit=amount,
                credit=Decimal("0"),
            )

            #CR Opening Balance Equity
            JournalLine.objects.create(
                entry=je,
                account=opening_equity_acct,
                debit=Decimal("0"),
                credit=amount,
            )

        return redirect('sowaf:customers')

    return render(request, 'customers_form.html', {'customer': customer})
# importing a customer sheet
# template for the download
def download_customers_template(request):
    wb = Workbook()
    ws = wb.active
    ws.title = "Customer Template"

    headers = [
        'name', 'company', 'email', 'phone', 'mobile', 'website', 'tin', 'balance', 'date_str', 'street1', 'street2', 'city', 'province', 'postal_code', 'country', 'notes', 'logo'
    ]
    ws.append(headers)

    with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        wb.save(tmp.name)
        tmp.seek(0)
        response = HttpResponse(tmp.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="customer_template.xlsx"'
        return response
def import_customers(request):
    
        if request.method == 'POST' and request.FILES.get('excel_file'):
            excel_file = request.FILES['excel_file']
        file_name = excel_file.name.lower()

        try:
            if file_name.endswith('.csv'):
                decoded_file = excel_file.read().decode('utf-8')
                io_string = io.StringIO(decoded_file)
                reader = csv.reader(io_string)
                next(reader)  # Skip header row

                for row in reader:
                   name, company, email, phone, mobile, website, tin, balance, date_str, street1, street2, city, province, postal_code, country, actions, notes, logo = row
                   Newcustomer.objects.create(
                        customer_name=name,
                        company_name=company,
                        email=email,
                        phone_number=phone,
                        mobile_number=mobile,
                        website=website,
                        tin_number=tin,
                        opening_balance=balance,
                        registration_date=date_str,
                        street_one=street1,
                        street_two=street2,
                        city=city,
                        province=province,
                        postal_code=postal_code,
                        country=country,
                        notes=notes,
                    )
                   if logo:
                        image_path = os.path.join(settings.MEDIA_ROOT, 'uploads', logo)
                        if os.path.exists(image_path):
                            with open(image_path, 'rb') as f:
                                Newcustomer.logo.save(logo, File(f), save=False)
                        else:
                            messages.warning(request, f"Image file '{logo}' not found.")
                            Newcustomer.save()
            
            elif file_name.endswith('.xlsx'):
                wb = openpyxl.load_workbook(excel_file)
                sheet = wb.active

                for row in sheet.iter_rows(min_row=2, values_only=True):
                    name, company, email, phone, mobile, website, tin, balance, date_str, street1, street2, city, province, postal, country, actions, notes, logo = row
                    Newcustomer.objects.create(
                        customer_name=name,
                        company_name=company,
                        email=email,
                        phone_number=phone,
                        mobile_number=mobile,
                        website=website,
                        tin_number=tin,
                        opening_balance=balance,
                        registration_date=date_str,
                        street_one=street1,
                        street_two=street2,
                        city=city,
                        province=province,
                        postal_code=postal,
                        country=country,
                        notes=notes,
                    )
                    if logo:
                        image_path = os.path.join(settings.MEDIA_ROOT, 'uploads', logo)
                        if os.path.exists(image_path):
                            with open(image_path, 'rb') as f:
                                Newcustomer.logo.save(logo, File(f), save=True)
                        else:
                            messages.warning(request, f"Image file '{logo}' not found.")
                            Newcustomer.save()
            else:
                messages.error(request, "Unsupported file type. Please upload a .csv or .xlsx file.")
                return redirect('sowaf:customers')
        except Exception as e:
            messages.error(request, f"Import failed: {str(e)}")
            return redirect('sowaf:customers')
        return redirect('sowaf:customers')   
# clients view

def clients(request):
    clients = Newclient.objects.all()
    return render(request, 'Clients.html', {'clients':clients})

# client form view
def _parse_date(val):
    if not val:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    return None
def add_client(request):
    if request.method == 'POST':
        # logo validation (PNG, <=1MB) — keep your existing rules
        logo = request.FILES.get('logo')
        if logo:
            if not logo.name.lower().endswith('.png'):
                messages.error(request, "Only PNG files are allowed for the logo.")
                return redirect(request.path)
            if logo.size > 1 * 1024 * 1024:
                messages.error(request, "Logo file size must not exceed 1MB.")
                return redirect(request.path)

        # read fields
        company         = request.POST.get('company')
        phone           = request.POST.get('phone')
        company_email   = request.POST.get('company_email')
        address         = request.POST.get('address')
        country         = request.POST.get('country')
        reg_number      = request.POST.get('reg_number')
        start_date      = _parse_date(request.POST.get('start_date'))  # <-- fixed
        contact_name    = request.POST.get('contact_name')
        position        = request.POST.get('position')
        contact         = request.POST.get('contact')
        contact_email   = request.POST.get('contact_email')
        tin             = request.POST.get('tin')
        credit_limit    = request.POST.get('credit_limit')
        payment_terms   = request.POST.get('payment_terms')
        currency        = request.POST.get('currency')
        industry        = request.POST.get('industry')
        status          = request.POST.get('status')
        notes           = request.POST.get('notes')

        client = Newclient(
            logo=logo, company=company, phone=phone, company_email=company_email,
            address=address, country=country, reg_number=reg_number,
            start_date=start_date, contact_name=contact_name, position=position,
            contact=contact, contact_email=contact_email, tin=tin,
            credit_limit=credit_limit, payment_terms=payment_terms,
            currency=currency, industry=industry, status=status, notes=notes
        )
        client.save()

        # save actions
        save_action = request.POST.get('save_action')
        if save_action == 'save':
            return redirect('sowaf:clients')
        if save_action == 'save&new':
            return redirect('sowaf:add-client')
        elif save_action == 'save&close':
            return redirect('sowaf:clients')
        return redirect('sowaf:clients')

    return render(request, 'Clients_form.html', {})  # create flow

# editing the client

def edit_client(request, pk: int):
    client = get_object_or_404(Newclient, pk=pk)

    if request.method == 'POST':
        # optional logo replacement (keep old if none uploaded)
        logo = request.FILES.get('logo')
        if logo:
            if not logo.name.lower().endswith('.png'):
                messages.error(request, "Only PNG files are allowed for the logo.")
                return redirect(request.path)
            if logo.size > 1 * 1024 * 1024:
                messages.error(request, "Logo file size must not exceed 1MB.")
                return redirect(request.path)
            client.logo = logo  # replace

        # update fields
        client.company        = request.POST.get('company') or client.company
        client.phone          = request.POST.get('phone') or client.phone
        client.company_email  = request.POST.get('company_email') or client.company_email
        client.address        = request.POST.get('address') or client.address
        client.country        = request.POST.get('country') or client.country
        client.reg_number     = request.POST.get('reg_number') or client.reg_number
        client.start_date     = _parse_date(request.POST.get('start_date')) or client.start_date
        client.contact_name   = request.POST.get('contact_name') or client.contact_name
        client.position       = request.POST.get('position') or client.position
        client.contact        = request.POST.get('contact') or client.contact
        client.contact_email  = request.POST.get('contact_email') or client.contact_email
        client.tin            = request.POST.get('tin') or client.tin
        client.credit_limit   = request.POST.get('credit_limit') or client.credit_limit
        client.payment_terms  = request.POST.get('payment_terms') or client.payment_terms
        client.currency       = request.POST.get('currency') or client.currency
        client.industry       = request.POST.get('industry') or client.industry
        client.status         = request.POST.get('status') or client.status
        client.notes          = request.POST.get('notes') or client.notes

        client.save()

        save_action = request.POST.get('save_action')
        if save_action == 'save':
            return redirect('sowaf:clients')
        if save_action == 'save&new':

            return redirect('sowaf:add-client')
        elif save_action == 'save&close':

            return redirect('sowaf:clients')

        return redirect('sowaf:edit-client', pk=client.id)

    # GET: render same form, pre-filled
    return render(request, 'Clients_form.html', {"client": client})# client delete view
def delete_client(request, pk):
    client = get_object_or_404(Newclient, pk=pk)
    client.delete()
    return redirect('sowaf:clients')

# importing the client
def download_clients_template(request):
    wb = Workbook()
    ws = wb.active
    ws.title = "clients Template"

    headers = [
        'company', 'phone', 'company_email', 'address', 'country',
        'registration_number', 'start_date', 'contact_name',
        'position', 'contact', 'contact_email', 'tin', 'credit_limit',
        'payment_terms', 'currency', 'industry', 'status',
         'notes', 'logo'
    ]
    ws.append(headers)

    with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        wb.save(tmp.name)
        tmp.seek(0)
        response = HttpResponse(tmp.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="clients_template.xlsx"'
        return response
def handle_logo_upload(client, logo):
    if logo:
        image_path = os.path.join(settings.MEDIA_ROOT, 'uploads', logo)
        if os.path.exists(image_path):
            with open(image_path, 'rb') as f:
                client.logo.save(logo, File(f), save=True)
        else:
            messages.warning(None, f"Image file '{logo}' not found.")


def parse_start_date(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def import_clients(request):
    if request.method != 'POST' or 'excel_file' not in request.FILES:
        messages.error(request, "No file uploaded.")
        return redirect('sowaf:clients')

    excel_file = request.FILES['excel_file']
    file_name = excel_file.name.lower()

    try:
        if file_name.endswith('.csv'):
            decoded_file = excel_file.read().decode('utf-8')
            io_string = io.StringIO(decoded_file)
            reader = csv.reader(io_string)
            next(reader)  # Skip header row

            for row in reader:
                (
                    company, phone, company_email, address, country,
                    registration_number, start_date, contact_name,
                    position, contact, contact_email, tin, credit_limit,
                    payment_terms, currency, industry, status,
                    notes, logo
                ) = row

                client = Newclient.objects.create(
                    company=company,
                    phone=phone,
                    company_email=company_email,
                    address=address,
                    country=country,
                    reg_number=registration_number,
                    start_date=parse_start_date(start_date),
                    contact_name=contact_name,
                    position=position,
                    contact=contact,
                    contact_email=contact_email,
                    tin=tin,
                    credit_limit=credit_limit,
                    payment_terms=payment_terms,
                    currency=currency,
                    industry=industry,
                    status=status,
                    notes=notes,
                    logo=logo,
                )
                handle_logo_upload(client, logo)

        elif file_name.endswith('.xlsx'):
            wb = openpyxl.load_workbook(excel_file)
            sheet = wb.active

            for row in sheet.iter_rows(min_row=2, values_only=True):
                (
                    company, phone, company_email, address, country,
                    registration_number, start_date, contact_name,
                    position, contact, contact_email, tin, credit_limit,
                    payment_terms, currency, industry, status,
                    notes, logo
                ) = row

                client = Newclient.objects.create(
                    company=company,
                    phone=phone,
                    company_email=company_email,
                    address=address,
                    country=country,
                    reg_number=registration_number,
                    start_date=parse_start_date(start_date),
                    contact_name=contact_name,
                    position=position,
                    contact=contact,
                    contact_email=contact_email,
                    tin=tin,
                    credit_limit=credit_limit,
                    payment_terms=payment_terms,
                    currency=currency,
                    industry=industry,
                    status=status,
                    notes=notes,
                    logo=logo,
                )
                handle_logo_upload(client, logo)

        else:
            messages.error(request, "Unsupported file type. Please upload a .csv or .xlsx file.")
            return redirect('sowaf:clients')

        messages.success(request, "Client data imported successfully.")
        return redirect('sowaf:clients')

    except Exception as e:
        messages.error(request, f"Import failed: {str(e)}")
        return redirect('sowaf:clients')
def employee(request):
    employees = Newemployee.objects.all()

    return render(request, 'Employees.html', {'employees': employees})
# add employee form 
def add_employees(request):
    if request.method == 'POST':

        first_name = request.POST.get('first_name')
        last_name = request.POST.get('last_name')
        gender = request.POST.get('gender')
        dob = parse_date(request.POST.get('dob') or "")
        nationality = request.POST.get('nationality')
        nin_number = request.POST.get('nin_number')
        tin_number = request.POST.get('tin_number')

        profile_picture = request.FILES.get('profile_picture')
        if profile_picture:
            if not profile_picture.name.lower().endswith('.png'):
                messages.error(request, "Only PNG files are allowed for the profile picture.")
                return redirect(request.path)
            if profile_picture.size > 1048576:
                messages.error(request, "Profile picture file size must not exceed 1MB.")
                return redirect(request.path)

        phone_number = request.POST.get('phone_number')
        email_address = request.POST.get('email_address')
        residential_address = request.POST.get('residential_address')
        emergency_person = request.POST.get('emergency_person')
        emergency_contact = request.POST.get('emergency_contact')
        relationship = request.POST.get('relationship')

        job_title = request.POST.get('job_title')
        department = request.POST.get('department')
        employment_type = request.POST.get('employment_type')
        hire_date = parse_date(request.POST.get('hire_date') or "")
        supervisor = request.POST.get('supervisor')

        # ----- DECIMAL FIELDS (FIXED) -----
        raw_salary = request.POST.get('salary')
        try:
            salary = Decimal(raw_salary) if raw_salary else Decimal("0.00")
        except:
            salary = Decimal("0.00")

        raw_taxable = request.POST.get('taxable_allowances')
        try:
            taxable_allowances = Decimal(raw_taxable) if raw_taxable else Decimal("0.00")
        except:
            taxable_allowances = Decimal("0.00")

        raw_intaxable = request.POST.get('intaxable_allowances')
        try:
            intaxable_allowances = Decimal(raw_intaxable) if raw_intaxable else Decimal("0.00")
        except:
            intaxable_allowances = Decimal("0.00")
        # ----------------------------------

        payment_frequency = request.POST.get('payment_frequency')
        payment_method = request.POST.get('payment_method')
        bank_name = request.POST.get('bank_name')
        bank_account = request.POST.get('bank_account')
        bank_branch = request.POST.get('bank_branch')
        nssf_number = request.POST.get('nssf_number')
        insurance_provider = request.POST.get('insurance_provider')
        additional_notes = request.POST.get('additional_notes')
        doc_attachments = request.FILES.get('doc_attachments')

        employee = Newemployee(
            first_name=first_name,
            last_name=last_name,
            gender=gender,
            dob=dob,
            nationality=nationality,
            nin_number=nin_number,
            tin_number=tin_number,
            profile_picture=profile_picture,
            phone_number=phone_number,
            email_address=email_address,
            residential_address=residential_address,
            emergency_person=emergency_person,
            emergency_contact=emergency_contact,
            relationship=relationship,
            job_title=job_title,
            department=department,
            employment_type=employment_type,
            hire_date=hire_date,
            supervisor=supervisor,
            salary=salary,
            payment_frequency=payment_frequency,
            payment_method=payment_method,
            bank_name=bank_name,
            bank_account=bank_account,
            bank_branch=bank_branch,
            nssf_number=nssf_number,
            insurance_provider=insurance_provider,
            taxable_allowances=taxable_allowances,
            intaxable_allowances=intaxable_allowances,
            additional_notes=additional_notes,
            doc_attachments=doc_attachments,
        )

        employee.save()

        save_action = request.POST.get('save_action')
        if save_action == 'save&new':
            return redirect('sowaf:add-employee')
        if save_action in ['save', 'save&close']:
            return redirect('sowaf:employees')

    return render(request, 'employees_form.html')


def edit_employee(request, pk):
    employee = get_object_or_404(Newemployee, pk=pk)

    if request.method == 'POST':

        employee.first_name = request.POST.get('first_name', employee.first_name)
        employee.last_name = request.POST.get('last_name', employee.last_name)
        employee.gender = request.POST.get('gender', employee.gender)
        employee.dob = parse_date(request.POST.get('dob') or "")
        employee.nationality = request.POST.get('nationality', employee.nationality)
        employee.nin_number = request.POST.get('nin_number', employee.nin_number)
        employee.tin_number = request.POST.get('tin_number', employee.tin_number)

        employee.phone_number = request.POST.get('phone_number', employee.phone_number)
        employee.email_address = request.POST.get('email_address', employee.email_address)
        employee.residential_address = request.POST.get('residential_address', employee.residential_address)
        employee.emergency_person = request.POST.get('emergency_person', employee.emergency_person)
        employee.emergency_contact = request.POST.get('emergency_contact', employee.emergency_contact)
        employee.relationship = request.POST.get('relationship', employee.relationship)

        employee.job_title = request.POST.get('job_title', employee.job_title)
        employee.department = request.POST.get('department', employee.department)
        employee.employment_type = request.POST.get('employment_type', employee.employment_type)
        employee.hire_date = parse_date(request.POST.get('hire_date') or "")
        employee.supervisor = request.POST.get('supervisor', employee.supervisor)

        # ----- DECIMAL FIELDS (FIXED) -----
        raw_salary = request.POST.get('salary')
        try:
            employee.salary = Decimal(raw_salary) if raw_salary else Decimal("0.00")
        except:
            employee.salary = Decimal("0.00")

        raw_taxable = request.POST.get('taxable_allowances')
        try:
            employee.taxable_allowances = Decimal(raw_taxable) if raw_taxable else Decimal("0.00")
        except:
            employee.taxable_allowances = Decimal("0.00")

        raw_intaxable = request.POST.get('intaxable_allowances')
        try:
            employee.intaxable_allowances = Decimal(raw_intaxable) if raw_intaxable else Decimal("0.00")
        except:
            employee.intaxable_allowances = Decimal("0.00")
        # ----------------------------------

        employee.payment_frequency = request.POST.get('payment_frequency', employee.payment_frequency)
        employee.payment_method = request.POST.get('payment_method', employee.payment_method)
        employee.bank_name = request.POST.get('bank_name', employee.bank_name)
        employee.bank_account = request.POST.get('bank_account', employee.bank_account)
        employee.bank_branch = request.POST.get('bank_branch', employee.bank_branch)
        employee.nssf_number = request.POST.get('nssf_number', employee.nssf_number)
        employee.insurance_provider = request.POST.get('insurance_provider', employee.insurance_provider)
        employee.additional_notes = request.POST.get('additional_notes', employee.additional_notes)

        profile_picture = request.FILES.get('profile_picture')
        if profile_picture:
            if not profile_picture.name.lower().endswith('.png'):
                messages.error(request, "Only PNG files are allowed for the profile picture.")
                return redirect(request.path)
            if profile_picture.size > 1048576:
                messages.error(request, "Profile picture file size must not exceed 1MB.")
                return redirect(request.path)
            employee.profile_picture = profile_picture

        if 'doc_attachments' in request.FILES:
            employee.doc_attachments = request.FILES['doc_attachments']

        employee.save()
        return redirect('sowaf:employees')

    return render(request, 'employees_form.html', {'employee': employee})
# importing employees
def download_employees_template(request):
    wb = Workbook()
    ws = wb.active
    ws.title = "Employees Template"

    headers = [
        'first_name', 'last_name', 'gender', 'dob', 'nationality',
        'nin_number', 'tin_number', 'profile_picture', 'phone_number', 'email_address',
        'residential_address', 'emergency_person', 'emergency_contact', 'relationship',
        'job_title', 'department', 'employment_type', 'status', 'hire_date', 'supervisor',
        'salary', 'payment_frequency', 'payment_method', 'bank_name', 'bank_account',
        'bank_branch', 'nssf_number', 'insurance_provider', 'taxable_allowances',
        'intaxable_allowances', 'additional_notes'
    ]
    ws.append(headers)

    with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        wb.save(tmp.name)
        tmp.seek(0)
        response = HttpResponse(tmp.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="employees_template.xlsx"'
        return response
def handle_profile_picture_upload(employee, profile_picture):
    if profile_picture:
        image_path = os.path.join(settings.MEDIA_ROOT, 'uploads', profile_picture)
        if os.path.exists(image_path):
            with open(image_path, 'rb') as f:
                employee.profile_picture.save(profile_picture, File(f), save=True)
        else:
            messages.warning(None, f"Image file '{profile_picture}' not found.")


# Parse DOB (multiple formats)
def parse_dob_safe(dob):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(dob), fmt).date()
        except (ValueError, TypeError):
            continue
    return None

# Parse Hire Date (multiple formats)
def parse_hire_date_safe(hire_date):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(hire_date), fmt).date()
        except (ValueError, TypeError):
            continue
    return None

def import_employees(request):
    if request.method != 'POST' or 'excel_file' not in request.FILES:
        messages.error(request, "No file uploaded.")
        return redirect('sowaf:employees')

    excel_file = request.FILES['excel_file']
    file_name = excel_file.name.lower()

    try:
        if file_name.endswith('.csv'):
            decoded_file = excel_file.read().decode('utf-8')
            io_string = io.StringIO(decoded_file)
            reader = csv.reader(io_string)
            next(reader)  # Skip header row

            for row in reader:
                (
                first_name,last_name,gender,dob,nationality,nin_number,tin_number,profile_picture,phone_number,email_address,residential_address,emergency_person,emergency_contact,relationship,job_title,department,employment_type,status,hire_date,supervisor,salary,payment_frequency,payment_method,bank_name,bank_account,bank_branch,nssf_number,insurance_provider,taxable_allowances,intaxable_allowances,additional_notes
                ) = row
                
                dob = parse_dob_safe(dob)
                hire_date = parse_hire_date_safe(hire_date)
                profile_picture = profile_picture.strip() if profile_picture else ''


                employee = Newemployee.objects.create(
                    first_name=first_name,
                    last_name=last_name,
                    gender=gender,
                    dob=dob,
                    nationality=nationality,
                    nin_number=nin_number,
                    tin_number=tin_number,
                    profile_picture=profile_picture,
                    phone_number=str(phone_number).rstrip('.0') if phone_number else '',
                    email_address=email_address,
                    residential_address=residential_address,
                    emergency_person=emergency_person,
                    emergency_contact=emergency_contact,
                    relationship=relationship,
                    job_title=job_title,
                    department=department,
                    employment_type=employment_type,
                    status=status,
                    hire_date=hire_date,
                    supervisor=supervisor,
                    salary=salary,
                    payment_frequency=payment_frequency,
                    payment_method=payment_method,
                    bank_name=bank_name,
                    bank_account=bank_account,
                    bank_branch=bank_branch,
                    nssf_number=nssf_number,
                    insurance_provider=insurance_provider,
                    taxable_allowances=taxable_allowances,
                    intaxable_allowances=intaxable_allowances,
                    additional_notes=additional_notes,
                )
                handle_profile_picture_upload(employee, profile_picture)

        elif file_name.endswith('.xlsx'):
            wb = openpyxl.load_workbook(excel_file)
            sheet = wb.active

            for row in sheet.iter_rows(min_row=2, values_only=True):
                (
                first_name,last_name,gender,dob,nationality,nin_number,tin_number,profile_picture,phone_number,email_address,residential_address,emergency_person,emergency_contact,relationship,job_title,department,employment_type,status,hire_date,supervisor,salary,payment_frequency,payment_method,bank_name,bank_account,bank_branch,nssf_number,insurance_provider,taxable_allowances,intaxable_allowances,additional_notes
                ) = row

                dob = parse_dob_safe(dob)
                hire_date = parse_hire_date_safe(hire_date)
                profile_picture = profile_picture.strip() if profile_picture else ''

                employee = Newemployee.objects.create(
                    first_name=first_name,
                    last_name=last_name,
                    gender=gender,
                    dob=dob,
                    nationality=nationality,
                    nin_number=nin_number,
                    tin_number=tin_number,
                    profile_picture=profile_picture,
                    phone_number=str(phone_number).rstrip('.0') if phone_number else '',
                    email_address=email_address,
                    residential_address=residential_address,
                    emergency_person=emergency_person,
                    emergency_contact=emergency_contact,
                    relationship=relationship,
                    job_title=job_title,
                    department=department,
                    employment_type=employment_type,
                    status=status,
                    hire_date=hire_date,
                    supervisor=supervisor,
                    salary=salary,
                    payment_frequency=payment_frequency,
                    payment_method=payment_method,
                    bank_name=bank_name,
                    bank_account=bank_account,
                    bank_branch=bank_branch,
                    nssf_number=nssf_number,
                    insurance_provider=insurance_provider,
                    taxable_allowances=taxable_allowances,
                    intaxable_allowances=intaxable_allowances,
                    additional_notes=additional_notes,
                )
                handle_profile_picture_upload(employee, profile_picture)

        else:
            messages.error(request, "Unsupported file type. Please upload a .csv or .xlsx file.")
            return redirect('sowaf:employees')

        messages.success(request, "employee data imported successfully.")
        return redirect('sowaf:employees')

    except Exception as e:
        messages.error(request, f"Import failed: {str(e)}")
        return redirect('sowaf:employees')


# supplier view
def supplier(request):
    suppliers = Newsupplier.objects.all()
     
    return render(request, 'Supplier.html', {'suppliers':suppliers})
# making the row active or inactive
def make_inactive_supplier(request, pk):
    supplier = get_object_or_404(Newsupplier, pk=pk)
    supplier.is_active = False
    supplier.save()
    return redirect('sowaf:suppliers')
# reactivating
def make_active_supplier(request, pk):
    supplier = get_object_or_404(Newsupplier, pk=pk)
    supplier.is_active = True
    supplier.save()
    return redirect('sowaf:suppliers')
#add new supplier form view
@transaction.atomic
def add_supplier(request):
    if request.method == 'POST':
        logo = request.FILES.get('logo')
        if logo:
            if not logo.name.lower().endswith('.png'):
                messages.error(request, "Only PNG files are allowed for the logo.")
                return redirect(request.path)
            if logo.size > 1048576:
                messages.error(request, "logo file size must not exceed 1MB.")
                return redirect(request.path)

        company_name    = request.POST.get('company_name')
        supplier_type   = request.POST.get('supplier_type')
        contact_person  = request.POST.get('contact_person')
        contact_position= request.POST.get('contact_position')
        contact         = request.POST.get('contact')
        email           = request.POST.get('email')

        raw_balance = request.POST.get('open_balance')
        try:
            open_balance = Decimal(raw_balance) if raw_balance else Decimal("0.00")
        except:
            open_balance = Decimal("0.00")

        website        = request.POST.get('website')
        address1       = request.POST.get('address1')
        address2       = request.POST.get('address2')
        city           = request.POST.get('city')
        state          = request.POST.get('state')
        zip_code       = request.POST.get('zip_code')
        country        = request.POST.get('country')
        bank           = request.POST.get('bank')
        bank_account   = request.POST.get('bank_account')
        bank_branch    = request.POST.get('bank_branch')
        payment_terms  = request.POST.get('payment_terms')
        currency       = request.POST.get('currency')
        payment_method = request.POST.get('payment_method')
        tin            = request.POST.get('tin')
        reg_number     = request.POST.get('reg_number')
        attachments    = request.FILES.get('attachments')

        new_supplier = Newsupplier(
            logo=logo,
            company_name=company_name,
            supplier_type=supplier_type,
            contact_person=contact_person,
            contact_position=contact_position,
            contact=contact,
            email=email,
            open_balance=open_balance,
            website=website,
            address1=address1,
            address2=address2,
            city=city,
            state=state,
            zip_code=zip_code,
            country=country,
            bank=bank,
            bank_account=bank_account,
            bank_branch=bank_branch,
            payment_terms=payment_terms,
            currency=currency,
            payment_method=payment_method,
            tin=tin,
            reg_number=reg_number,
            attachments=attachments,
        )

        new_supplier.save()

        # === SUPPLIER OPENING BALANCE -> GENERAL LEDGER ===
        if open_balance != Decimal("0.00"):
            # 1) Find / create the control accounts we need

            # Accounts Payable control account (liability)
            ap_account = (
                Account.objects.filter(account_type="ACCOUNTS_PAYABLE").first()
                or Account.objects.filter(detail_type__icontains="payable").first()
            )
            if not ap_account:
                # Hard fail is ok here so you immediately notice mis-setup
                raise Exception(
                    "Please create an 'Accounts Payable' control account first "
                    "(account_type='ACCOUNTS_PAYABLE' or detail_type containing 'payable')."
                )

            # Opening Balance Equity (same as we used for COA and customer OBs)
            opening_equity_acct, _ = Account.objects.get_or_create(
                account_name="Opening Balance Equity",
                account_type="OWNER_EQUITY",
                defaults={
                    "detail_type": "Opening balances",
                    "is_active": True,
                },
            )

            # 2) Create the journal entry
            je = JournalEntry.objects.create(
                date=timezone.now().date(),
                description=f"Opening balance for supplier {new_supplier.company_name}",
                source_type="SUPP_OPEN_BALANCE",
                source_id=new_supplier.id,
            )

            amount = abs(open_balance)

            # A supplier opening balance means WE OWE THEM:
            #   DR Opening Balance Equity
            #   CR Accounts Payable
            JournalLine.objects.create(
                entry=je,
                account=opening_equity_acct,
                debit=amount,
                credit=Decimal("0.00"),
            )
            JournalLine.objects.create(
                entry=je,
                account=ap_account,
                debit=Decimal("0.00"),
                credit=amount,
            )

        # === Save actions as you had ===
        save_action = request.POST.get('save_action')
        if save_action == 'save':
            return redirect('sowaf:suppliers')          
        if save_action == 'save&new':
            return redirect('sowaf:add-suppliers')     
        elif save_action == 'save&close':
            return redirect('sowaf:suppliers')

    return render(request, 'suppliers_entry_form.html', {})

# edit supplier

@transaction.atomic
def edit_supplier(request, pk):
    supplier = get_object_or_404(Newsupplier, pk=pk)

    if request.method == 'POST':
        supplier.company_name     = request.POST.get('company_name', supplier.company_name)
        supplier.supplier_type    = request.POST.get('supplier_type', supplier.supplier_type)
        supplier.contact_person   = request.POST.get('contact_person', supplier.contact_person)
        supplier.contact_position = request.POST.get('contact_position', supplier.contact_position)
        supplier.contact          = request.POST.get('contact', supplier.contact)
        supplier.email            = request.POST.get('email', supplier.email)

        raw_balance = request.POST.get('open_balance')
        try:
            new_open_balance = Decimal(raw_balance) if raw_balance else Decimal("0.00")
        except:
            new_open_balance = Decimal("0.00")

        supplier.open_balance = new_open_balance

        supplier.website       = request.POST.get('website', supplier.website)
        supplier.address1      = request.POST.get('address1', supplier.address1)
        supplier.address2      = request.POST.get('address2', supplier.address2)
        supplier.city          = request.POST.get('city', supplier.city)
        supplier.state         = request.POST.get('state', supplier.state)
        supplier.zip_code      = request.POST.get('zip_code', supplier.zip_code)
        supplier.country       = request.POST.get('country', supplier.country)
        supplier.bank          = request.POST.get('bank', supplier.bank)
        supplier.bank_account  = request.POST.get('bank_account', supplier.bank_account)
        supplier.bank_branch   = request.POST.get('bank_branch', supplier.bank_branch)
        supplier.payment_terms = request.POST.get('payment_terms', supplier.payment_terms)
        supplier.currency      = request.POST.get('currency', supplier.currency)
        supplier.payment_method= request.POST.get('payment_method', supplier.payment_method)
        supplier.tin           = request.POST.get('tin', supplier.tin)
        supplier.reg_number    = request.POST.get('reg_number', supplier.reg_number)

        # Only update logo if a new one is uploaded
        logo = request.FILES.get('logo')
        if logo:
            if not logo.name.lower().endswith('.png'):
                messages.error(request, "Only PNG files are allowed for the logo.")
                return redirect(request.path)
            if logo.size > 1048576:
                messages.error(request, "logo file size must not exceed 1MB.")
                return redirect(request.path)
            supplier.logo = logo

        if 'attachments' in request.FILES:
            supplier.attachments = request.FILES['attachments']

        supplier.save()

        # === UPDATE SUPPLIER OPENING BALANCE JOURNAL ENTRY ===
        je = JournalEntry.objects.filter(
            source_type="SUPP_OPEN_BALANCE",
            source_id=supplier.id
        ).first()

        if new_open_balance == Decimal("0.00"):
            # If balance now zero, remove any existing opening entry
            if je:
                je.delete()
        else:
            # Need AP + Opening Balance Equity accounts
            ap_account = (
                Account.objects.filter(account_type="ACCOUNTS_PAYABLE").first()
                or Account.objects.filter(detail_type__icontains="payable").first()
            )
            if not ap_account:
                raise Exception(
                    "Please create an 'Accounts Payable' control account first "
                    "(account_type='ACCOUNTS_PAYABLE' or detail_type containing 'payable')."
                )

            opening_equity_acct, _ = Account.objects.get_or_create(
                account_name="Opening Balance Equity",
                account_type="OWNER_EQUITY",
                defaults={
                    "detail_type": "Opening balances",
                    "is_active": True,
                },
            )

            amount = abs(new_open_balance)
            as_of  = timezone.now().date()

            if not je:
                je = JournalEntry.objects.create(
                    date=as_of,
                    description=f"Opening balance for supplier {supplier.company_name}",
                    source_type="SUPP_OPEN_BALANCE",
                    source_id=supplier.id,
                )
            else:
                je.date = as_of
                je.description = f"Opening balance for supplier {supplier.company_name}"
                je.save()
                # Wipe old lines and rebuild
                JournalLine.objects.filter(entry=je).delete()

            # DR Opening Balance Equity, CR Accounts Payable
            JournalLine.objects.create(
                entry=je,
                account=opening_equity_acct,
                debit=amount,
                credit=Decimal("0.00"),
            )
            JournalLine.objects.create(
                entry=je,
                account=ap_account,
                debit=Decimal("0.00"),
                credit=amount,
            )

        return redirect('sowaf:suppliers')

    return render(request, 'suppliers_entry_form.html', {'supplier': supplier})


# supplier detail page 
def supplier_detail(request, pk):
    supplier = get_object_or_404(Newsupplier, pk=pk)
    tab = request.GET.get("tab", "transactions")

    # ---- Aggregates for banners/summary ----
    today = timezone.now().date()

    bills_qs = Bill.objects.filter(supplier=supplier).annotate(
        total_amount_dec=Cast("total_amount", DecimalField(max_digits=18, decimal_places=2))
    )

    expenses_qs = Expense.objects.filter(payee_supplier=supplier).annotate(
        total_amount_dec=Cast("total_amount", DecimalField(max_digits=18, decimal_places=2))
    )

    cheques_qs = Cheque.objects.filter(payee_supplier=supplier).annotate(
        total_amount_dec=Cast("total_amount", DecimalField(max_digits=18, decimal_places=2))
    )

    bills_total = bills_qs.aggregate(
        t=Coalesce(Sum("total_amount_dec"), Value(Decimal("0.00")))
    )["t"] or Decimal("0.00")

    paid_total = (
        (expenses_qs.aggregate(t=Coalesce(Sum("total_amount_dec"), Value(Decimal("0.00"))))["t"] or Decimal("0.00"))
        + (cheques_qs.aggregate(t=Coalesce(Sum("total_amount_dec"), Value(Decimal("0.00"))))["t"] or Decimal("0.00"))
    )

    # open balance ≈ total bills - payments/cheques (floor at 0)
    open_balance = bills_total - paid_total
    if open_balance < 0:
        open_balance = Decimal("0.00")

    overdue_bills_total = bills_qs.filter(due_date__lt=today).aggregate(
        t=Coalesce(Sum("total_amount_dec"), Value(Decimal("0.00")))
    )["t"] or Decimal("0.00")

    count_bills = bills_qs.count()

    # ---- Transaction rows (Bills + Expenses + Cheques) ----
    rows = []

    for b in bills_qs.order_by("-bill_date", "-id"):
        rows.append({
            "id": b.id,
            "date": getattr(b, "bill_date", None),
            "type": "Bill",
            "no": b.bill_no or f"BILL-{b.id:04d}",
            "party": supplier.company_name or supplier.contact_person or "",
            "memo": (getattr(b, "memo", "") or "")[:140],
            "amount": b.total_amount_dec or Decimal("0.00"),
            "status": getattr(b, "status", "Open") or "Open",
            "edit_url":  reverse("expenses:bill-edit", args=[b.id]),
            "view_url":  reverse("expenses:bill-edit", args=[b.id]),   # change if you have a detail route
            "print_url": "#",
        })

    for e in expenses_qs.order_by("-payment_date", "-id"):
        rows.append({
            "id": e.id,
            "date": getattr(e, "payment_date", None),
            "type": "Expense",
            "no": getattr(e, "ref_no", "") or f"EXP-{e.id:04d}",
            "party": supplier.company_name or supplier.contact_person or "",
            "memo": (getattr(e, "memo", "") or "")[:140],
            "amount": e.total_amount_dec or Decimal("0.00"),
            "status": "Paid",
            "edit_url":  reverse("expenses:expense-edit", args=[e.id]),
            "view_url":  reverse("expenses:expense-edit", args=[e.id]),
            "print_url": "#",
        })

    for c in cheques_qs.order_by("-payment_date", "-id"):
        rows.append({
            "id": c.id,
            "date": getattr(c, "payment_date", None),
            "type": "Cheque",
            "no": getattr(c, "cheque_no", "") or f"CHQ-{c.id:04d}",
            "party": supplier.company_name or supplier.contact_person or "",
            "memo": (getattr(c, "memo", "") or "")[:140],
            "amount": c.total_amount_dec or Decimal("0.00"),
            "status": "Paid",
            "edit_url":  reverse("expenses:cheque-edit", args=[c.id]),
            "view_url":  reverse("expenses:cheque-edit", args=[c.id]),
            "print_url": "#",
        })

    # sort combined rows desc by date, then by type/id
    rows.sort(key=lambda r: (r["date"] or today, r["type"], r["id"]), reverse=True)

    context = {
        "supplier": supplier,
        "tab": tab,
        "open_balance": open_balance,
        "overdue_balance": overdue_bills_total,
        "count_bills": count_bills,
        "transactions_rows": rows,
    }
    return render(request, "supplier_detail.html", context)

# importing suppliers
def download_suppliers_template(request):
    wb = Workbook()
    ws = wb.active
    ws.title = "Suppliers Template"

    headers = [
        'logo','company_name','supplier_type','status','contact_person','contact_position', 'contact','email','open_balance','website','address1','address2','city','state','zip_code','country','bank','bank_account','bank_branch','payment_terms','currency','payment_method','tin','reg_number','tax_rate',
    ]
    ws.append(headers)

    with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        wb.save(tmp.name)
        tmp.seek(0)
        response = HttpResponse(tmp.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="suppliers_template.xlsx"'
        return response
def handle_logo_upload(supplier, logo):
    if logo:
        image_path = os.path.join(settings.MEDIA_ROOT, 'uploads', logo)
        if os.path.exists(image_path):
            with open(image_path, 'rb') as f:
                supplier.logo.save(logo, File(f), save=True)
        else:
            messages.warning(None, f"Image file '{logo}' not found.")

def import_suppliers(request):
    if request.method != 'POST' or 'excel_file' not in request.FILES:
        messages.error(request, "No file uploaded.")
        return redirect('sowaf:suppliers')

    excel_file = request.FILES['excel_file']
    file_name = excel_file.name.lower()

    try:
        if file_name.endswith('.csv'):
            decoded_file = excel_file.read().decode('utf-8')
            io_string = io.StringIO(decoded_file)
            reader = csv.reader(io_string)
            next(reader)
            for row in reader:
                (
                    logo, company_name, supplier_type, status, contact_person, contact_position, contact, email, open_balance, website, address1, address2, city, state, zip_code, country, bank, bank_account,bank_branch,payment_terms,currency,payment_method,tin,reg_number,tax_rate,
                )
                
                logo = logo.strip() if logo else ''


                supplier = Newsupplier.objects.create(
                    logo=logo,
                    company_name=company_name,
                    supplier_type=supplier_type,
                    status=status,
                    contact_person=contact_person,
                    contact_position=contact_position, 
                    contact=contact,
                    email=email,
                    open_balance=open_balance,
                    website=website,
                    address1=address1,
                    address2=address2,
                    city=city,
                    state=state,
                    zip_code=zip_code,
                    country=country,
                    bank=bank,
                    bank_account=bank_account,
                    bank_branch=bank_branch,
                    payment_terms=payment_terms,
                    currency=currency,
                    payment_method=payment_method,
                    tin=tin,
                    reg_number=reg_number,
                    tax_rate=tax_rate,
                )
                handle_logo_upload(supplier, logo)

        elif file_name.endswith('.xlsx'):
            wb = openpyxl.load_workbook(excel_file)
            sheet = wb.active

            for row in sheet.iter_rows(min_row=2, values_only=True):
                (
                logo, company_name, supplier_type, status, contact_person, contact_position, contact, email, open_balance, website, address1, address2, city, state, zip_code, country, bank, bank_account,bank_branch,payment_terms,currency,payment_method,tin,reg_number,tax_rate,
                ) = row


                logo = logo.strip() if logo else ''

                supplier = Newsupplier.objects.create(
                    logo=logo,
                    company_name=company_name,
                    supplier_type=supplier_type,
                    status=status,
                    contact_person=contact_person,
                    contact_position=contact_position, 
                    contact=contact,
                    email=email,
                    open_balance=open_balance,
                    website=website,
                    address1=address1,
                    address2=address2,
                    city=city,
                    state=state,
                    zip_code=zip_code,
                    country=country,
                    bank=bank,
                    bank_account=bank_account,
                    bank_branch=bank_branch,
                    payment_terms=payment_terms,
                    currency=currency,
                    payment_method=payment_method,
                    tin=tin,
                    reg_number=reg_number,
                    tax_rate=tax_rate,
                )
                handle_logo_upload(supplier, logo)

        else:
            messages.error(request, "Unsupported file type. Please upload a .csv or .xlsx file.")
            return redirect('sowaf:suppliers')

        messages.success(request, "supplier data imported successfully.")
        return redirect('sowaf:suppliers')

    except Exception as e:
        messages.error(request, f"Import failed: {str(e)}")
        return redirect('sowaf:suppliers')
# tasks view
def tasks(request):

    return render(request, 'tasks.html', {})
# taxes view
def taxes(request):

    return render(request, 'Taxes.html', {})


# millecious view
def miscellaneous(request):

    return render(request, 'Miscellaneous.html', {})
# reports view 
def reports(request):

    return render(request, 'Reports.html', {})
