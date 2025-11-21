# Create your views here.
from decimal import Decimal, InvalidOperation
from django.contrib import messages
from django.db.models.functions import Coalesce
from django.db.models import Sum, Value, DecimalField, Prefetch
from django.views.decorators.csrf import csrf_exempt
import json
from django.urls import reverse
from django.db import transaction
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.utils import timezone
from django.db.models import Q, Sum
from django.core.paginator import Paginator
from .models import Expense, ExpenseCategoryLine, ExpenseItemLine,ColumnPreference,Bill, BillCategoryLine, BillItemLine
from .models import Cheque, ChequeCategoryLine, ChequeItemLine

from sowaf.models import Newcustomer, Newsupplier
from accounts.models import Account,JournalEntry
from inventory.models import Product,Pclass
from .utils import generate_unique_ref_no
from .services import post_expense_to_gl,post_bill_to_ledger,post_cheque_to_ledger

# Expenses view

DEFAULT_ACCOUNTS_COL_PREFS = {
    "payment_date": True,
    "payee_name": True,
    "payee_supplier": True,
    "payment_account": True,
    "payment_method": True,
    "ref_no": True,
    "memo": True,
    "attachments": True,  # keep actions togglable too
}
def _cat_label_from_lines(cat_lines, item_lines):
    """
    Build a single Category label for the row:
      - '--Split--' if more than one line
      - category account name for a single category line
      - product name for a single item line
      - '' if no lines
    Assumes related lines were prefetched.
    """
    cats = list(cat_lines) if hasattr(cat_lines, "__iter__") else list(cat_lines.all())
    items = list(item_lines) if hasattr(item_lines, "__iter__") else list(item_lines.all())
    n = len(cats) + len(items)
    if n == 0:
        return ""
    if n > 1:
        return "--Split--"
    if len(cats) == 1:
        return getattr(cats[0].category, "account_name", "")
    if len(items) == 1:
        return getattr(items[0].product, "name", "")
    return ""

def expenses(request):
    # ---------------- Expenses ----------------
    exp_qs = (
        Expense.objects
        .select_related("payee_supplier")
        .prefetch_related("cat_lines__category", "item_lines__product")
        .order_by("-payment_date", "-id")
    )

    # Optional: cache line counts for old template bits
    for e in exp_qs:
        e._total_lines = len(list(e.cat_lines.all())) + len(list(e.item_lines.all()))

    # ---------------- Bills ----------------
    bill_qs = (
        Bill.objects
        .select_related("supplier")
        .prefetch_related("category_lines__category", "item_lines__product")
        .order_by("-bill_date", "-id")
    )

    # ---------------- Cheques ----------------
    cheque_qs = (
        Cheque.objects
        .select_related("payee_supplier", "bank_account")
        .prefetch_related("category_lines__category", "item_lines__product")
        .order_by("-payment_date", "-id")
    )

    # ---------------- Normalize into one list ----------------
    rows = []

    for e in exp_qs:
        rows.append({
            "id": e.id,
            "kind": "Expense",
            "date": e.payment_date,
            "number": getattr(e, "ref_no", "") or "",
            "payee": (e.payee_supplier.company_name if e.payee_supplier else (e.payee_name or "")),
            "category": _cat_label_from_lines(e.cat_lines.all(), e.item_lines.all()),
            "total_before_tax": e.total_amount,   # adjust if you add tax columns later
            "sales_tax": Decimal("0.00"),
            "total": e.total_amount,
            "edit_url": reverse("expenses:expense-edit", args=[e.id]),
        })

    for b in bill_qs:
        rows.append({
            "id": b.id,
            "kind": "Bill",
            "date": b.bill_date,
            "number": b.bill_no or "",
            "payee": (b.supplier.company_name if b.supplier else (b.supplier_name or "")),
            "category": _cat_label_from_lines(b.category_lines.all(), b.item_lines.all()),
            "total_before_tax": b.total_amount,
            "sales_tax": Decimal("0.00"),
            "total": b.total_amount,
            "edit_url": reverse("expenses:bill-edit", args=[b.id]),
        })

    for c in cheque_qs:
        rows.append({
            "id": c.id,
            "kind": "Cheque",
            "date": c.payment_date,
            "number": c.cheque_no or "",
            "payee": (c.payee_supplier.company_name if c.payee_supplier else (c.payee_name or "")),
            "category": _cat_label_from_lines(c.category_lines.all(), c.item_lines.all()),
            "total_before_tax": c.total_amount,
            "sales_tax": Decimal("0.00"),
            "total": c.total_amount,
            "edit_url": reverse("expenses:cheque-edit", args=[c.id])  # make sure this url exists
        })

    # Sort all transactions together: newest date, then newest id
    rows = sorted(rows, key=lambda r: (r["date"], r["id"]), reverse=True)

    # ---------------- Column prefs (unchanged) ----------------
    if getattr(request.user, "is_authenticated", False):
        prefs, _ = ColumnPreference.objects.get_or_create(
            user=request.user,
            table_name="accounts",
            defaults={"preferences": DEFAULT_ACCOUNTS_COL_PREFS},
        )
        merged_prefs = {**DEFAULT_ACCOUNTS_COL_PREFS, **(prefs.preferences or {})}
    else:
        merged_prefs = DEFAULT_ACCOUNTS_COL_PREFS

    # Keep old 'expenses' for compatibility; add 'transactions' for the unified list
    return render(request, "expenses.html", {
        "expenses": exp_qs,            # existing var (if anything still uses it)
        "transactions": rows,          # unified list for the table
        "column_prefs": merged_prefs,
    })

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
# adding an expense
def _dec(v, default="0.00"):
    try:
        return Decimal(str(v or default))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)

def add_expense(request):
    if request.method == "POST":
        try:
            with transaction.atomic():
                # Header
                payee_name      = request.POST.get("payee_name") or ""
                supplier_id     = request.POST.get("payee_supplier") or ""
                payment_account = get_object_or_404(Account, pk=request.POST.get("payment_account"))
                payment_date    = request.POST.get("payment_date") or timezone.localdate()
                payment_method  = request.POST.get("payment_method") or "cash"
                ref_no          = request.POST.get("ref_no") or ""
                location        = request.POST.get("location") or ""
                memo            = request.POST.get("memo") or ""
                attachment      = request.FILES.get("attachments")

                supplier = Newsupplier.objects.filter(pk=supplier_id).first() if supplier_id else None
                # ensure we have a valid 8-digit numeric ref; if not, generate one
                if not (len(ref_no) == 8 and ref_no.isdigit()):
                    ref_no = generate_unique_ref_no()
        # guard against the (very rare) race where the same ref got used meanwhile
                if Expense.objects.filter(ref_no=ref_no).exists():
                    ref_no = generate_unique_ref_no()
                exp = Expense.objects.create(
                    payee_name=payee_name,
                    payee_supplier=supplier,
                    payment_account=payment_account,
                    payment_date=payment_date,
                    payment_method=payment_method,
                    ref_no=ref_no,
                    location=location,
                    memo=memo,
                    attachments=attachment,
                )

                total = Decimal("0.00")

                # -------- Category lines --------
                cat_category_ids = request.POST.getlist("cat_category[]")
                cat_descs        = request.POST.getlist("cat_desc[]")
                cat_amounts      = request.POST.getlist("cat_amount[]")
                cat_billable     = set(request.POST.getlist("cat_billable[]"))  # contains row idx strings
                cat_customer_ids = request.POST.getlist("cat_customer[]")
                cat_class_ids    = request.POST.getlist("cat_class[]")

                for idx, cat_id in enumerate(cat_category_ids):
                    if not cat_id:
                        continue
                    category = Account.objects.filter(pk=cat_id).first()
                    if not category:
                        continue

                    amt = _dec(cat_amounts[idx])
                    if amt == 0:
                        continue

                    is_bill = str(idx) in cat_billable
                    customer = Newcustomer.objects.filter(pk=cat_customer_ids[idx] or None).first()
                    klass    = Pclass.objects.filter(pk=cat_class_ids[idx] or None).first()

                    ExpenseCategoryLine.objects.create(
                        expense=exp, category=category,
                        description=cat_descs[idx],
                        amount=amt, is_billable=is_bill,
                        customer=customer, class_field=klass
                    )
                    total += amt

                # -------- Item lines --------
                item_product_ids = request.POST.getlist("item_product[]")
                item_descs       = request.POST.getlist("item_desc[]")
                item_qtys        = request.POST.getlist("item_qty[]")
                item_rates       = request.POST.getlist("item_rate[]")
                item_amounts     = request.POST.getlist("item_amount[]")
                item_billable    = set(request.POST.getlist("item_billable[]"))
                item_customer_ids= request.POST.getlist("item_customer[]")
                item_class_ids   = request.POST.getlist("item_class[]")

                for idx, prod_id in enumerate(item_product_ids):
                    if not prod_id:
                        continue
                    product = Product.objects.filter(pk=prod_id).first()
                    if not product:
                        continue

                    qty  = _dec(item_qtys[idx], "0")
                    rate = _dec(item_rates[idx], "0")
                    amt  = _dec(item_amounts[idx]) or (qty * rate)

                    is_bill = str(idx) in item_billable
                    customer = Newcustomer.objects.filter(pk=item_customer_ids[idx] or None).first()
                    klass    = Pclass.objects.filter(pk=item_class_ids[idx] or None).first()

                    ExpenseItemLine.objects.create(
                        expense=exp, product=product,
                        description=item_descs[idx],
                        qty=qty, rate=rate, amount=amt,
                        is_billable=is_bill, customer=customer, class_field=klass
                    )
                    total += amt

                exp.total_amount = total
                exp.save(update_fields=["total_amount"])
                # posting to chart of accounts
                post_expense_to_gl(exp)
                action = request.POST.get("save_action") or "save"
                if action == "save":
                    return redirect("expenses:expenses")
                if action == "save&new":
                    return redirect("expenses:add-expenses")
                # tweak ‘save&close’ destination as you wish:
                return redirect("expenses:expenses")

        except Exception as e:
            messages.error(request, f"Could not save expense: {e}")
    ref_no = generate_unique_ref_no()
    # GET: load form lists
    context = {
        "accounts": Account.objects.all().order_by("account_name"),
        "expense_accounts": Account.objects.filter(account_type__in=[
            "EXPENSE", "OTHER_EXPENSE", "COST_OF_SALES", "Expense", "Other Expenses", "Cost of Sales"
        ]).order_by("account_name"),
        "products": Product.objects.all().order_by("name"),
        "customers": Newcustomer.objects.all().order_by("customer_name"),
        "suppliers": Newsupplier.objects.all().order_by("company_name"),
        "classes": Pclass.objects.all().order_by("class_name"),
        "ref_no":ref_no,
        "payment_methods": Expense.PAYMENT_METHODS, 
    }
    return render(request, "expenses_form.html", context)
# expense list 
def expense_list(request):
    qs = (
        Expense.objects
        .select_related("payee_supplier", "payment_account")
        .prefetch_related("cat_lines__category", "item_lines__product")
        .order_by("-payment_date", "-id")
    )
    return render(request, "expenses_list.html", {"expenses": qs})
# expense detail
def expense_detail(request, pk: int):
    exp = get_object_or_404(
        Expense.objects
        .select_related("payee_supplier", "payment_account")
        .prefetch_related("cat_lines__category", "item_lines__product"),
        pk=pk
    )
    # Optional: get the journal if you linked it with FK expense
    je = JournalEntry.objects.filter(expense=exp).prefetch_related("lines__account").first()
    return render(request, "expense_detail.html", {"e": exp, "journal": je})
# expense edit
def expense_edit(request, pk: int):
    exp = get_object_or_404(
        Expense.objects
        .select_related("payee_supplier", "payment_account")
        .prefetch_related("cat_lines__category", "item_lines__product"),
        pk=pk
        
    )

    if request.method == "POST":
        try:
            with transaction.atomic():
                # ---- Header
                exp.payee_name      = request.POST.get("payee_name") or ""
                supplier_id         = request.POST.get("payee_supplier") or ""
                exp.payee_supplier  = Newsupplier.objects.filter(pk=supplier_id).first() if supplier_id else None
                exp.payment_account = get_object_or_404(Account, pk=request.POST.get("payment_account"))
                exp.payment_date    = request.POST.get("payment_date") or timezone.localdate()
                exp.payment_method  = request.POST.get("payment_method") or "cash"
                exp.ref_no          = request.POST.get("ref_no") or exp.ref_no or ""
                exp.location        = request.POST.get("location") or ""
                exp.memo            = request.POST.get("memo") or ""
                if request.FILES.get("attachments"):
                    exp.attachments = request.FILES["attachments"]
                exp.save()

                # ---- Replace lines
                ExpenseCategoryLine.objects.filter(expense=exp).delete()
                ExpenseItemLine.objects.filter(expense=exp).delete()

                total = Decimal("0.00")

                # Category lines
                cat_category_ids = request.POST.getlist("cat_category[]")
                cat_descs        = request.POST.getlist("cat_desc[]")
                cat_amounts      = request.POST.getlist("cat_amount[]")
                cat_billable     = set(request.POST.getlist("cat_billable[]"))
                cat_customer_ids = request.POST.getlist("cat_customer[]")
                cat_class_ids    = request.POST.getlist("cat_class[]")

                for idx, cat_id in enumerate(cat_category_ids):
                    if not cat_id:
                        continue
                    category = Account.objects.filter(pk=cat_id).first()
                    if not category:
                        continue
                    amt = _dec(cat_amounts[idx])
                    if amt == 0:
                        continue
                    is_bill = str(idx) in cat_billable
                    customer = Newcustomer.objects.filter(pk=cat_customer_ids[idx] or None).first()
                    klass    = Pclass.objects.filter(pk=cat_class_ids[idx] or None).first()

                    ExpenseCategoryLine.objects.create(
                        expense=exp, category=category,
                        description=cat_descs[idx],
                        amount=amt, is_billable=is_bill,
                        customer=customer, class_field=klass
                    )
                    total += amt

                # Item lines
                item_product_ids = request.POST.getlist("item_product[]")
                item_descs       = request.POST.getlist("item_desc[]")
                item_qtys        = request.POST.getlist("item_qty[]")
                item_rates       = request.POST.getlist("item_rate[]")
                item_amounts     = request.POST.getlist("item_amount[]")
                item_billable    = set(request.POST.getlist("item_billable[]"))
                item_customer_ids= request.POST.getlist("item_customer[]")
                item_class_ids   = request.POST.getlist("item_class[]")

                for idx, prod_id in enumerate(item_product_ids):
                    if not prod_id:
                        continue
                    product = Product.objects.filter(pk=prod_id).first()
                    if not product:
                        continue
                    qty  = _dec(item_qtys[idx], "0")
                    rate = _dec(item_rates[idx], "0")
                    amt  = _dec(item_amounts[idx]) or (qty * rate)

                    is_bill = str(idx) in item_billable
                    customer = Newcustomer.objects.filter(pk=item_customer_ids[idx] or None).first()
                    klass    = Pclass.objects.filter(pk=item_class_ids[idx] or None).first()

                    ExpenseItemLine.objects.create(
                        expense=exp, product=product,
                        description=item_descs[idx],
                        qty=qty, rate=rate, amount=amt,
                        is_billable=is_bill, customer=customer, class_field=klass
                    )
                    total += amt

                exp.total_amount = total
                exp.save(update_fields=["total_amount"])

                # ---- Rebuild journal (delete old + re-post)
                JournalEntry.objects.filter(expense=exp).delete()
                post_expense_to_gl(exp)
                return redirect("expenses:expense-detail", pk=exp.pk)

        except Exception as e:
            return redirect("expenses:add-expense")

    # GET → prefill context
    context = {
        "expense": exp,
        "accounts": Account.objects.all().order_by("account_name"),
        "expense_accounts": Account.objects.filter(account_type__in=[
            "EXPENSE", "OTHER_EXPENSE", "COST_OF_SALES", "Expense", "Other Expenses", "Cost of Sales"
        ]).order_by("account_name"),
        "products": Product.objects.all().order_by("name"),
        "customers": Newcustomer.objects.all().order_by("customer_name"),
        "suppliers": Newsupplier.objects.all().order_by("company_name"),
        "classes": Pclass.objects.all().order_by("class_name"),
        # existing lines to loop in the form:
        "cat_lines": exp.cat_lines.select_related("category", "customer", "class_field").all(),
        "item_lines": exp.item_lines.select_related("product", "customer", "class_field").all(),
        "payment_methods": Expense.PAYMENT_METHODS, 
    }
    return render(request, "expenses_form.html", context)

# end
# bill views

def _dec(v, default="0.00"):
    try:
        return Decimal(str(v if v not in (None, "") else default))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def generate_unique_bill_no(prefix="BILL"):
    """
    8-digit numeric suffix (like 00001234) with a prefix for readability. Ensures uniqueness.
    """
    base_date = timezone.now().strftime("%y%m")  # e.g., '2510'
    seed = f"{base_date}0001"
    suffix = int(seed)
    while True:
        candidate = f"{prefix}{suffix:08d}"
        if not Bill.objects.filter(bill_no=candidate).exists():
            return candidate
        suffix += 1

def _dec(v, default="0.00"):
    try:
        return Decimal(str(v if v not in (None, "",) else default))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)

def _parse_ymd(s, fallback=None):
    """Parse 'YYYY-MM-DD' safely; returns fallback if empty/invalid."""
    if not s:
        return fallback
    try:
        return timezone.datetime.fromisoformat(s).date()
    except Exception:
        return fallback

def generate_unique_bill_no():
    # Simple example. Replace with your existing generator if you already have one.
    last = Bill.objects.order_by("-id").first()
    base = 10000000 if not last else (int(str(last.bill_no or 0).strip()[-8:]) if str(last.bill_no or "").isdigit() else last.id) + 1
    return f"{base:08d}"

@transaction.atomic
def add_bill(request):
    if request.method == "POST":
        try:
            # ---------- Header ----------
            supplier_id      = request.POST.get("supplier_id") or ""   # from <select name="supplier_id">
            supplier_name    = request.POST.get("supplier") or ""      # optional text fallback if you keep it
            mailing_address  = request.POST.get("mailing_address") or ""
            terms            = request.POST.get("terms") or ""
            bill_date        = _parse_ymd(request.POST.get("bill_date"), timezone.localdate())
            due_date         = _parse_ymd(request.POST.get("due_date"))
            bill_no          = (request.POST.get("bill_no") or "").strip()
            location         = request.POST.get("location") or ""
            memo             = request.POST.get("memo") or ""
            attachment       = request.FILES.get("attachments")

            supplier = Newsupplier.objects.filter(pk=supplier_id).first() if supplier_id else None

            # Ensure a unique bill_no (auto-generate if missing/duplicate)
            if (not bill_no) or Bill.objects.filter(bill_no=bill_no).exists():
                bill_no = generate_unique_bill_no()

            bill = Bill.objects.create(
                supplier=supplier,
                supplier_name=None if supplier else supplier_name,
                mailing_address=mailing_address,
                terms=terms,
                bill_date=bill_date,
                due_date=due_date,
                bill_no=bill_no,
                location=location,
                memo=memo,
                attachments=attachment,
            )

            total = Decimal("0.00")

            # ---------- Category lines ----------
            cat_category_ids = request.POST.getlist("cat_category[]")
            cat_descs        = request.POST.getlist("cat_desc[]")
            cat_amounts      = request.POST.getlist("cat_amount[]")
            cat_billable     = set(request.POST.getlist("cat_billable[]"))  # row indices as strings
            cat_customer_ids = request.POST.getlist("cat_customer[]")
            cat_class_ids    = request.POST.getlist("cat_class[]")

            for idx, acc_id in enumerate(cat_category_ids):
                if not acc_id:
                    continue
                account = Account.objects.filter(pk=acc_id).first()
                if not account:
                    continue

                amt = _dec(cat_amounts[idx])
                if amt <= 0:
                    continue

                is_bill  = str(idx) in cat_billable
                customer = Newcustomer.objects.filter(pk=(cat_customer_ids[idx] or None)).first()
                klass    = Pclass.objects.filter(pk=(cat_class_ids[idx] or None)).first()

                BillCategoryLine.objects.create(
                    bill=bill, category=account,
                    description=(cat_descs[idx] or ""),
                    amount=amt, is_billable=is_bill,
                    customer=customer, class_field=klass
                )
                total += amt

            # ---------- Item lines ----------
            item_product_ids  = request.POST.getlist("item_product[]")
            item_descs        = request.POST.getlist("item_desc[]")
            item_qtys         = request.POST.getlist("item_qty[]")
            item_rates        = request.POST.getlist("item_rate[]")
            item_amounts      = request.POST.getlist("item_amount[]")
            item_billable     = set(request.POST.getlist("item_billable[]"))
            item_customer_ids = request.POST.getlist("item_customer[]")
            item_class_ids    = request.POST.getlist("item_class[]")

            for idx, prod_id in enumerate(item_product_ids):
                if not prod_id:
                    continue
                product = Product.objects.filter(pk=prod_id).first()
                if not product:
                    continue

                qty  = _dec(item_qtys[idx], "0")
                rate = _dec(item_rates[idx], "0")
                amt  = _dec(item_amounts[idx]) if item_amounts[idx] else (qty * rate)
                if amt <= 0:
                    continue

                is_bill  = str(idx) in item_billable
                customer = Newcustomer.objects.filter(pk=(item_customer_ids[idx] or None)).first()
                klass    = Pclass.objects.filter(pk=(item_class_ids[idx] or None)).first()

                BillItemLine.objects.create(
                    bill=bill, product=product,
                    description=(item_descs[idx] or ""),
                    qty=qty, rate=rate, amount=amt,
                    is_billable=is_bill, customer=customer, class_field=klass
                )
                total += amt

            if total <= 0:
                # undo header if nothing valid was entered
                bill.delete()
                messages.error(request, "No valid lines to save on this Bill.")
                return redirect("expenses:add-bill")

            bill.total_amount = total
            bill.save(update_fields=["total_amount"])

            # ---------- Post to GL (DR expenses / CR A/P) ----------
            post_bill_to_ledger(bill)

            # Redirect options
            action = request.POST.get("save_action") or "save"
            if action == "save&new":
                messages.success(request, "Bill saved.")
                return redirect("expenses:add-bill")
            messages.success(request, "Bill saved.")
            return redirect("expenses:bills-list")

        except Exception as e:
            messages.error(request, f"Could not save bill: {e}")

    # GET: choices
    context = {
        "expense_accounts": Account.objects.filter(
            account_type__in=[
                "EXPENSE", "OTHER_EXPENSE", "COST_OF_SALES",
                "Expense", "Other Expenses", "Cost of Sales"
            ]
        ).order_by("account_name"),
        "all_accounts": Account.objects.all().order_by("account_name"),
        "products": Product.objects.all().order_by("name"),
        "customers": Newcustomer.objects.all().order_by("customer_name"),
        "suppliers": Newsupplier.objects.all().order_by("company_name"),
        "classes": Pclass.objects.all().order_by("class_name"),
        "generated_bill_no": generate_unique_bill_no(),
    }
    return render(request, "bill_form.html", context)
# bill edit

@transaction.atomic
def edit_bill(request, pk: int):
    bill = get_object_or_404(
        Bill.objects.select_related("supplier")
            .prefetch_related("category_lines__category",
                              "item_lines__product"),
        pk=pk
    )

    if request.method == "POST":
        try:
            # ---------- Header ----------
            supplier_id      = request.POST.get("supplier_id") or ""
            supplier_manual  = request.POST.get("supplier") or ""
            bill.mailing_address = request.POST.get("mailing_address") or ""
            bill.terms           = request.POST.get("terms") or ""
            bill.bill_date       = _parse_ymd(request.POST.get("bill_date"), bill.bill_date or timezone.localdate())
            bill.due_date        = _parse_ymd(request.POST.get("due_date"))
            new_bill_no          = (request.POST.get("bill_no") or "").strip()
            bill.location        = request.POST.get("location") or ""
            bill.memo            = request.POST.get("memo") or ""

            # Keep the old bill_no unless user actually changed it; if changed, ensure unique
            if new_bill_no and new_bill_no != (bill.bill_no or ""):
                if Bill.objects.exclude(pk=bill.pk).filter(bill_no=new_bill_no).exists():
                    messages.error(request, "Bill No. already exists. Please use another number.")
                    return redirect("expenses:bill-edit", pk=bill.pk)
                bill.bill_no = new_bill_no

            # Supplier (FK preferred; fallback to typed name)
            supplier = Newsupplier.objects.filter(pk=supplier_id).first() if supplier_id else None
            bill.supplier = supplier
            bill.supplier_name = None if supplier else supplier_manual

            # Attachment (optional replace)
            if request.FILES.get("attachments"):
                bill.attachments = request.FILES["attachments"]

            bill.save()

            # ---------- Replace lines ----------
            BillCategoryLine.objects.filter(bill=bill).delete()
            BillItemLine.objects.filter(bill=bill).delete()

            total = Decimal("0.00")

            # Category lines
            cat_category_ids = request.POST.getlist("cat_category[]")
            cat_descs        = request.POST.getlist("cat_desc[]")
            cat_amounts      = request.POST.getlist("cat_amount[]")
            cat_billable     = set(request.POST.getlist("cat_billable[]"))  # row indices as strings
            cat_customer_ids = request.POST.getlist("cat_customer[]")
            cat_class_ids    = request.POST.getlist("cat_class[]")

            for idx, acc_id in enumerate(cat_category_ids):
                if not acc_id:
                    continue
                account = Account.objects.filter(pk=acc_id).first()
                if not account:
                    continue

                amt = _dec(cat_amounts[idx])
                if amt <= 0:
                    continue

                is_bill  = str(idx) in cat_billable
                customer = Newcustomer.objects.filter(pk=(cat_customer_ids[idx] or None)).first()
                # If you have Pclass model, import it at top like in add_bill (omitted here to keep snippet compact)
              
                klass    = Pclass.objects.filter(pk=(cat_class_ids[idx] or None)).first()

                BillCategoryLine.objects.create(
                    bill=bill, category=account,
                    description=(cat_descs[idx] or ""),
                    amount=amt, is_billable=is_bill,
                    customer=customer, class_field=klass
                )
                total += amt

            # Item lines
            item_product_ids  = request.POST.getlist("item_product[]")
            item_descs        = request.POST.getlist("item_desc[]")
            item_qtys         = request.POST.getlist("item_qty[]")
            item_rates        = request.POST.getlist("item_rate[]")
            item_amounts      = request.POST.getlist("item_amount[]")
            item_billable     = set(request.POST.getlist("item_billable[]"))
            item_customer_ids = request.POST.getlist("item_customer[]")
            item_class_ids    = request.POST.getlist("item_class[]")

            for idx, prod_id in enumerate(item_product_ids):
                if not prod_id:
                    continue
                product = Product.objects.filter(pk=prod_id).first()
                if not product:
                    continue

                qty  = _dec(item_qtys[idx], "0")
                rate = _dec(item_rates[idx], "0")
                amt  = _dec(item_amounts[idx]) if item_amounts[idx] else (qty * rate)
                if amt <= 0:
                    continue

                is_bill  = str(idx) in item_billable
                customer = Newcustomer.objects.filter(pk=(item_customer_ids[idx] or None)).first()
                klass    = Pclass.objects.filter(pk=(item_class_ids[idx] or None)).first()

                BillItemLine.objects.create(
                    bill=bill, product=product,
                    description=(item_descs[idx] or ""),
                    qty=qty, rate=rate, amount=amt,
                    is_billable=is_bill, customer=customer, class_field=klass
                )
                total += amt

            if total <= 0:
                messages.error(request, "No valid lines found; bill not updated.")
                return redirect("expenses:bill-edit", pk=bill.pk)

            bill.total_amount = total
            bill.save(update_fields=["total_amount"])

            # ---------- Re-post to GL ----------
            # If your Bill model has a FK to JournalEntry (recommended):
            if hasattr(bill, "journal_entry_id") and bill.journal_entry_id:
                old = JournalEntry.objects.filter(pk=bill.journal_entry_id).first()
                if old:
                    old.lines.all().delete()
                    old.delete()
                bill.journal_entry = None
                bill.save(update_fields=["journal_entry"])

            je = post_bill_to_ledger(bill)

            if hasattr(bill, "journal_entry"):
                bill.journal_entry = je
                bill.save(update_fields=["journal_entry"])

            messages.success(request, "Bill updated.")
            action = request.POST.get("save_action") or "save"
            if action == "save&new":
                return redirect("expenses:add-bill")
            return redirect("expenses:bills-list")

        except Exception as e:
            messages.error(request, f"Could not update bill: {e}")

    # ---------- GET (prefill) ----------
    context = {
        "bill": bill,
        "suppliers": Newsupplier.objects.all().order_by("company_name"),
        "expense_accounts": Account.objects.filter(
            account_type__in=[
                "COST_OF_SALES", "EXPENSE", "OTHER_EXPENSE",
                "Cost of Sales", "Expenses", "Other Expenses"
            ]
        ).order_by("account_name"),
        "all_accounts": Account.objects.all().order_by("account_name"),
        "products": Product.objects.all().order_by("name"),
        "customers": Newcustomer.objects.all().order_by("customer_name"),
        "classes": Pclass.objects.all().order_by("class_name"),
        "cat_lines": BillCategoryLine.objects.filter(bill=bill).select_related("category","customer","class_field"),
        "item_lines": BillItemLine.objects.filter(bill=bill).select_related("product","customer","class_field"),
    }
    return render(request, "bill_form.html", context)
# bill list
def bills_list(request):
    """
    Bills list with search, date filter and pagination.
    """
    today = timezone.localdate()
    qs = (
        Bill.objects
        .select_related("supplier")
        .order_by("-bill_date", "-id")
    )

    # ----- Filters (GET) -----
    q = request.GET.get("q", "").strip()
    if q:
        qs = qs.filter(
            Q(bill_no__icontains=q) |
            Q(location__icontains=q) |
            Q(supplier__company_name__icontains=q)
        )

    date_from = request.GET.get("from", "")
    date_to   = request.GET.get("to", "")
    if date_from:
        qs = qs.filter(bill_date__gte=date_from)
    if date_to:
        qs = qs.filter(bill_date__lte=date_to)

    # Simple status chip (Open/Overdue/Closed) computed on the fly:
    # If you have a stored status field, you can display that instead.
    rows = []
    for b in qs:
        status = "Open"
        if b.due_date and b.due_date < today:
            status = "Overdue"
        # if you later add payments + balance logic, set "Closed" when fully paid
        rows.append((b, status))

    # Totals (for the current filtered set)
    totals = qs.aggregate(
        grand=Coalesce(Sum("total_amount"), Value(Decimal("0.00"), output_field=DecimalField(max_digits=18, decimal_places=2))),
    )
    # Keep keys your template expects; if you don't track them on the model, set 0
    totals["subtotal"] = Decimal("0.00")
    totals["tax"] = Decimal("0.00")
    # ----- Pagination -----
    paginator = Paginator(rows, 25)  # 25 per page
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    context = {
        "page_obj": page_obj,
        "q": q,
        "from": date_from,
        "to": date_to,
        "totals": totals,
        "count_all": qs.count(),
    }
    return render(request, "bill_list.html", context)
# bill detail

def bill_detail(request, pk):
    bill = get_object_or_404(
        Bill.objects.select_related("supplier").prefetch_related(
            Prefetch(
                "category_lines",
                queryset=BillCategoryLine.objects.select_related("category", "customer", "class_field"),
            ),
            Prefetch(
                "item_lines",
                queryset=BillItemLine.objects.select_related("product", "customer", "class_field"),
            ),
        ),
        pk=pk,
    )

    cat_total  = bill.category_lines.aggregate(s=Sum("amount"))["s"] or Decimal("0.00")
    item_total = bill.item_lines.aggregate(s=Sum("amount"))["s"] or Decimal("0.00")
    subtotal   = cat_total + item_total

    context = {
        "bill": bill,
        "cat_total": cat_total,
        "item_total": item_total,
        "subtotal": subtotal,
    }
    return render(request, "bill_detail.html", context)

# end




def add_time_activity(request):
   
    return render(request, 'time_activity_form.html', {})

def purchase_order(request):
   
    return render(request, 'purchase_order_form.html', {})
def supplier_credit(request):
   
    return render(request, 'supplier_credit_form.html', {})

def pay_down_credit(request):
   
    return render(request, 'pay_down_credit_form.html', {})
def import_bills(request):
   
    return render(request, 'import_bills_form.html', {})
def credit_card(request):
   
    return render(request, 'credit_card_credit_form.html', {})
#

# cheque view


def _dec(v, default="0.00"):
    try:
        return Decimal(str(v or default))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)

def generate_unique_cheque_no():
    # Simple sequential fallback: last cheque id + 1, padded
    last = Cheque.objects.order_by("-id").first()
    nxt = (last.id + 1) if last else 1
    return f"{nxt:06d}"

def add_cheque(request):
    if request.method == "POST":
        try:
            with transaction.atomic():
                supplier_id     = request.POST.get("payee_supplier") or ""
                payee_name      = request.POST.get("payee_name") or ""
                bank_account_id = request.POST.get("bank_account")
                mailing_address = request.POST.get("mailing_address") or ""
                payment_date    = request.POST.get("payment_date") or timezone.localdate()
                cheque_no       = request.POST.get("cheque_no") or ""
                location        = request.POST.get("location") or ""
                memo            = request.POST.get("memo") or ""
                attachment      = request.FILES.get("attachments")

                bank_account = get_object_or_404(Account, pk=bank_account_id)
                supplier     = Newsupplier.objects.filter(pk=supplier_id).first() if supplier_id else None

                # ensure unique cheque no
                if not cheque_no or Cheque.objects.filter(cheque_no=cheque_no).exists():
                    cheque_no = generate_unique_cheque_no()

                chq = Cheque.objects.create(
                    payee_name=payee_name,
                    payee_supplier=supplier,
                    bank_account=bank_account,
                    mailing_address=mailing_address,
                    payment_date=payment_date,
                    cheque_no=cheque_no,
                    location=location,
                    memo=memo,
                    attachments=attachment,
                )

                total = Decimal("0.00")

                # Category lines
                cat_category_ids = request.POST.getlist("cat_category[]")
                cat_descs        = request.POST.getlist("cat_desc[]")
                cat_amounts      = request.POST.getlist("cat_amount[]")
                cat_billable     = set(request.POST.getlist("cat_billable[]"))
                cat_customer_ids = request.POST.getlist("cat_customer[]")
                cat_class_ids    = request.POST.getlist("cat_class[]")

                for idx, acc_id in enumerate(cat_category_ids):
                    if not acc_id:
                        continue
                    account = Account.objects.filter(pk=acc_id).first()
                    if not account:
                        continue
                    amt = _dec(cat_amounts[idx])
                    if amt <= 0:
                        continue
                    customer = Newcustomer.objects.filter(pk=cat_customer_ids[idx] or None).first()
                    klass    = Pclass.objects.filter(pk=cat_class_ids[idx] or None).first()
                    is_bill  = str(idx) in cat_billable

                    ChequeCategoryLine.objects.create(
                        cheque=chq, category=account, description=cat_descs[idx],
                        amount=amt, is_billable=is_bill, customer=customer, class_field=klass
                    )
                    total += amt

                # Item lines
                item_product_ids = request.POST.getlist("item_product[]")
                item_descs       = request.POST.getlist("item_desc[]")
                item_qtys        = request.POST.getlist("item_qty[]")
                item_rates       = request.POST.getlist("item_rate[]")
                item_amounts     = request.POST.getlist("item_amount[]")
                item_billable    = set(request.POST.getlist("item_billable[]"))
                item_customer_ids= request.POST.getlist("item_customer[]")
                item_class_ids   = request.POST.getlist("item_class[]")

                for idx, prod_id in enumerate(item_product_ids):
                    if not prod_id:
                        continue
                    product = Product.objects.filter(pk=prod_id).first()
                    if not product:
                        continue
                    qty  = _dec(item_qtys[idx], "0")
                    rate = _dec(item_rates[idx], "0")
                    amt  = _dec(item_amounts[idx]) or (qty * rate)
                    if amt <= 0:
                        continue
                    customer = Newcustomer.objects.filter(pk=item_customer_ids[idx] or None).first()
                    klass    = Pclass.objects.filter(pk=item_class_ids[idx] or None).first()
                    is_bill  = str(idx) in item_billable

                    ChequeItemLine.objects.create(
                        cheque=chq, product=product, description=item_descs[idx],
                        qty=qty, rate=rate, amount=amt,
                        is_billable=is_bill, customer=customer, class_field=klass
                    )
                    total += amt

                chq.total_amount = total
                chq.save(update_fields=["total_amount"])

                # post to GL: DR expenses, CR bank
                post_cheque_to_ledger(chq)

                messages.success(request, "Cheque saved.")
                action = request.POST.get("save_action") or "save"
                if action == "save&new":
                    return redirect("expenses:add-cheque")
                return redirect("expenses:expenses")
        except Exception as e:
            messages.error(request, f"Could not save cheque: {e}")

    context = {
    "suppliers": Newsupplier.objects.all().order_by("company_name"),

    # Broaden the filter to catch your actual labels in account_type/detail_type.
    "bank_accounts": (
        Account.objects
        .filter(
            Q(account_type__iexact="BANK") |
            Q(account_type__iexact="CASH") |
            Q(account_type__icontains="bank") |
            Q(account_type__icontains="cash") |
            Q(detail_type__icontains="bank") |
            Q(detail_type__icontains="cash")
        )
        .order_by("account_name")
    ),
    "expense_accounts": Account.objects.filter(
        account_type__in=[
            "EXPENSE", "OTHER_EXPENSE", "COST_OF_SALES",
            "Expense", "Other Expenses", "Cost of Sales"
        ]
    ).order_by("account_name"),
    "products": Product.objects.all().order_by("name"),
    "customers": Newcustomer.objects.all().order_by("customer_name"),
    "classes": Pclass.objects.all().order_by("class_name"),
    "generated_cheque_no": generate_unique_cheque_no(),
    "today": timezone.localdate(),
    }
    return render(request, "cheque_form.html", context)

