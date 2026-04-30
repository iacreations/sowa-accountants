# inventory/accounting.py
from collections import defaultdict
from django.db import models
from decimal import Decimal, ROUND_HALF_UP
from django.db import transaction
from django.utils import timezone
from tenancy.models import Company
from accounts.models import JournalEntry, JournalLine, Account
from inventory.models import InventoryMovement, Product
from inventory.services import resolve_location_from_doc, get_default_location, PURCHASE_SOURCE_TYPES
DEC0 = Decimal("0.00")
_Q2 = Decimal("0.01")  # quantize target for 2 decimal places
_Q0 = Decimal("1")     # quantize target for whole numbers (UGX has no cents)


# -----------------------------
# Helpers
# -----------------------------
def _dec(x) -> Decimal:
    if x is None:
        return DEC0
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _clear_inventory_movements(source_type: str, source_id: int):
    InventoryMovement.objects.filter(source_type=source_type, source_id=source_id).delete()


def _delete_journal_entry_if_exists(obj):
    """
    obj must have a journal_entry OneToOne field.
    """
    if getattr(obj, "journal_entry_id", None):
        obj.journal_entry.delete()
        obj.journal_entry = None


def _create_journal_entry(*, date, description, source_type, source_id):
    return JournalEntry.objects.create(
        date=date,
        description=description,
        source_type=source_type,
        source_id=source_id,
    )


def _add_line(*, je: JournalEntry, account: Account, debit=DEC0, credit=DEC0, supplier=None, customer=None):
    return JournalLine.objects.create(
        entry=je,
        account=account,
        debit=_dec(debit).quantize(_Q2, rounding=ROUND_HALF_UP),
        credit=_dec(credit).quantize(_Q2, rounding=ROUND_HALF_UP),
        supplier=supplier,
        customer=customer,
    )


def _recalc_product_qty_avg(product: Product):
    """
    Kept for compatibility (not used). If you later want to rebuild everything from ledger,
    you can implement it fully. For now we update qty/avg_cost live on stock-in.
    """
    InventoryMovement.objects.filter(product=product).aggregate(
        in_qty_sum=models.Sum("qty_in"),
        out_qty_sum=models.Sum("qty_out"),
    )


def _find_account_by_name_contains(text: str, company=None):
    qs = Account.objects.filter(is_active=True, account_name__icontains=text)
    company_id = getattr(company, "id", company) if company is not None else None
    if company_id:
        qs = qs.filter(company_id=company_id)
    return qs.order_by("account_name").first()


def _fallback_inventory_asset_account(product: Product, company=None):
    """
    QuickBooks-style fallback when the product has no inventory_asset_account set.

    Priority:
      1) product.inventory_asset_account (if set)
      2) Any active account with name like Inventory/Stock/Merchandise
      3) If still none, fallback to product.expense_account (your request: some inventory may fall under expenses)
      4) Any active expense-like account by name "Cost of Sales"/"Expenses"/"Expense"
      5) None (caller decides what to do)
    """
    company = company or getattr(product, "company", None)
    inv_acc = getattr(product, "inventory_asset_account", None)
    if inv_acc:
        return inv_acc

    inv_acc = _find_account_by_name_contains("Inventory", company) or _find_account_by_name_contains("Stock", company) or _find_account_by_name_contains("Merchandise", company)
    if inv_acc:
        return inv_acc

    exp_acc = getattr(product, "expense_account", None)
    if exp_acc:
        return exp_acc

    exp_acc = _find_account_by_name_contains("Cost of Sales", company) or _find_account_by_name_contains("Cost of Goods", company) or _find_account_by_name_contains("COGS", company) \
              or _find_account_by_name_contains("Expenses", company) or _find_account_by_name_contains("Expense", company)
    return exp_acc


def _fallback_cogs_account(product: Product, company=None):
    """
    Fallback COGS for Inventory sales posting.

    Priority:
      1) product.cogs_account (if set)
      2) product.expense_account (allowed in your 3-level COA setup)
      3) Any active account by name "Cost of Sales"/"Cost of Goods"/"COGS"
      4) Any "Expenses"/"Expense"
    """
    company = company or getattr(product, "company", None)
    cogs = getattr(product, "cogs_account", None)
    if cogs:
        return cogs

    exp = getattr(product, "expense_account", None)
    if exp:
        return exp

    cogs = _find_account_by_name_contains("Cost of Sales", company) or _find_account_by_name_contains("Cost of Goods", company) or _find_account_by_name_contains("COGS", company)
    if cogs:
        return cogs

    return _find_account_by_name_contains("Expenses", company) or _find_account_by_name_contains("Expense", company)


def _fallback_ap_account(supplier, company=None):
    """
    AP priority:
      1) supplier.ap_account (if exists)
      2) CoA account named exactly "Accounts Payable"
      3) CoA contains "Accounts Payable" / "Payable"
    """
    company = company or getattr(supplier, "company", None)
    company_id = getattr(company, "id", company) if company is not None else None
    if supplier and getattr(supplier, "ap_account_id", None):
        return supplier.ap_account

    qs = Account.objects.filter(account_name__iexact="Accounts Payable", is_active=True)
    if company_id:
        qs = qs.filter(company_id=company_id)
    ap = qs.first()
    if ap:
        return ap

    return _find_account_by_name_contains("Accounts Payable", company) or _find_account_by_name_contains("Payable", company)


def _fallback_ar_account(customer, company=None):
    """
    AR priority:
      1) customer.ar_account (if exists)
      2) CoA account named exactly "Accounts Receivable"
      3) CoA contains "Accounts Receivable" / "Receivable"
    """
    company = company or getattr(customer, "company", None)
    company_id = getattr(company, "id", company) if company is not None else None
    if customer and getattr(customer, "ar_account_id", None):
        return customer.ar_account

    qs = Account.objects.filter(account_name__iexact="Accounts Receivable", is_active=True)
    if company_id:
        qs = qs.filter(company_id=company_id)
    ar = qs.first()
    if ar:
        return ar

    return _find_account_by_name_contains("Accounts Receivable", company) or _find_account_by_name_contains("Receivable", company)


def _fallback_sales_account(product: Product, company=None):
    """
    Income fallback:
      1) product.income_account
      2) "Sales"
      3) account contains "Sales"/"Revenue"
    """
    company = company or getattr(product, "company", None)
    company_id = getattr(company, "id", company) if company is not None else None
    inc = getattr(product, "income_account", None)
    if inc:
        return inc

    qs = Account.objects.filter(account_name__iexact="Sales", is_active=True)
    if company_id:
        qs = qs.filter(company_id=company_id)
    sales = qs.first()
    if sales:
        return sales

    return _find_account_by_name_contains("Sales", company) or _find_account_by_name_contains("Revenue", company)


# -----------------------------
# Stock In (Bills/Expenses)
# -----------------------------
def _apply_stock_in(product: Product, qty_in: Decimal, unit_cost: Decimal):
    """
    Update product.quantity and FIFO layers after a stock-in movement.
    The new movement is already saved to DB before this is called.
    """
    from django.db.models import Sum as _Sum
    from inventory.fifo import rebuild_layers_from_movements

    agg = product.movements.aggregate(tin=_Sum("qty_in"), tout=_Sum("qty_out"))
    product.quantity = _dec(agg["tin"]) - _dec(agg["tout"])
    product.save(update_fields=["quantity"])

    rebuild_layers_from_movements(product, company=getattr(product, "company", None))


def _get_or_create_ap_control_account(company=None):
    """Returns the A/P control account, creating if missing."""
    company_id = getattr(company, "id", company) if company is not None else None
    qs = Account.objects.filter(is_active=True, detail_type__iexact="Accounts Payable (A/P)")
    if company_id is not None:
        qs = qs.filter(company_id=company_id)
    acc = qs.first()
    if acc:
        return acc
    create_kwargs = dict(
        account_name="Accounts Payable",
        account_number="2000",
        account_type="CURRENT_LIABILITY",
        detail_type="Accounts Payable (A/P)",
        is_subaccount=False,
        parent=None,
        opening_balance=DEC0,
        as_of=timezone.localdate(),
        is_active=True,
    )
    if company_id is not None:
        create_kwargs["company_id"] = company_id
    return Account.objects.create(**create_kwargs)


def _get_or_create_supplier_ap_subaccount(supplier, company=None):
    """Creates/gets a supplier subaccount under A/P control (subledger)."""
    company = company or getattr(supplier, "company", None)
    company_id = getattr(company, "id", company) if company is not None else None
    ap_control = _get_or_create_ap_control_account(company=company)
    name = (getattr(supplier, "company_name", None) or "").strip() or f"Supplier {supplier.id}"
    qs = Account.objects.filter(parent=ap_control, account_name__iexact=name, is_active=True)
    if company_id is not None:
        qs = qs.filter(company_id=company_id)
    acc = qs.first()
    if not acc:
        create_kwargs = dict(
            account_name=name,
            account_type=ap_control.account_type,
            detail_type="Supplier Subledger (A/P)",
            is_active=True,
            is_subaccount=True,
            parent=ap_control,
            opening_balance=DEC0,
            as_of=timezone.localdate(),
        )
        if company_id is not None:
            create_kwargs["company_id"] = company_id
        acc = Account.objects.create(**create_kwargs)
    if getattr(supplier, "ap_account_id", None) != acc.id:
        supplier.ap_account = acc
        supplier.save(update_fields=["ap_account"])
    return acc


def _fallback_expense_account(product, company=None):
    """Expense/COGS account for non-inventory items. Always returns an account if any exists."""
    if product:
        acc = getattr(product, "expense_account", None) or getattr(product, "cogs_account", None)
        if acc:
            return acc
    company_id = getattr(company, "id", company) if company is not None else None
    for term in ("Cost of Goods", "Cost of Sales", "Cost of Good", "COGS", "Expense"):
        qs = Account.objects.filter(is_active=True, account_name__icontains=term)
        if company_id:
            qs = qs.filter(company_id=company_id)
        acc = qs.first()
        if acc:
            return acc
    # Last resort: any expense-type account for this company
    qs = Account.objects.filter(
        is_active=True,
        account_type__in=["OPERATING_EXPENSE", "INVESTING_EXPENSE", "FINANCING_EXPENSE", "INCOME_TAX_EXPENSE"],
    )
    if company_id:
        qs = qs.filter(company_id=company_id)
    return qs.first()


# -----------------------------
# Unified Bill Posting (GL + Inventory)
# -----------------------------
def post_bill_to_gl(bill):
    """
    BILL posting (unified – handles ALL line types):

      Category lines:          DR category account
      Inventory item lines:    DR Inventory Asset + stock-in movement + weighted avg update
      Non-inventory item lines: DR Expense/COGS account
      Credit:                  CR Supplier A/P Subaccount

    Sets bill.journal_entry, is_posted, posted_at.
    """
    from expenses.models import BillCategoryLine, BillItemLine

    company = bill.company
    company_id = getattr(company, "id", company)
    source_type = "BILL"
    source_id = bill.id
    post_date = bill.bill_date or timezone.localdate()
    supplier = bill.supplier if bill.supplier_id else None
    stock_location = resolve_location_from_doc(bill)

    total = _dec(bill.total_amount)
    if total <= 0:
        if bill.journal_entry_id:
            bill.journal_entry.delete()
            bill.journal_entry = None
            bill.is_posted = False
            bill.posted_at = None
            bill.save(update_fields=["journal_entry", "is_posted", "posted_at"])
        return

    if not bill.supplier_id:
        raise ValueError("Bill must have a Supplier selected to post to Accounts Payable subledger.")

    with transaction.atomic():
        _clear_inventory_movements(source_type, source_id)
        _delete_journal_entry_if_exists(bill)

        vendor_name = getattr(supplier, "company_name", "") if supplier else ""
        je = _create_journal_entry(
            date=post_date,
            description=f"Bill {bill.bill_no}" + (f" – {vendor_name}" if vendor_name else ""),
            source_type=source_type,
            source_id=source_id,
        )
        # set company on JE
        if hasattr(je, "company_id"):
            je.company_id = company_id
            je.save(update_fields=["company"])

        dr_total = DEC0

        # 1) Category lines → DR the chosen category account
        for cl in BillCategoryLine.objects.filter(bill=bill).select_related("category"):
            acc = cl.category
            amt = _dec(cl.amount)
            if acc and amt > 0:
                _add_line(je=je, account=acc, debit=amt, supplier=supplier)
                dr_total += amt

        # 2) Item lines
        for ln in BillItemLine.objects.filter(bill=bill).select_related("product"):
            product = ln.product
            amt = _dec(ln.amount) if _dec(ln.amount) > 0 else (_dec(ln.qty) * _dec(ln.rate))
            amt = amt.quantize(_Q2, rounding=ROUND_HALF_UP)
            if amt <= 0:
                continue

            if product and getattr(product, 'track_inventory', False):
                # Inventory: DR Inventory Asset, stock-in movement, weighted avg
                qty = _dec(ln.qty)
                unit_cost = _dec(ln.rate)
                value = (qty * unit_cost if qty > 0 else amt).quantize(_Q2, rounding=ROUND_HALF_UP)

                inv_acc = _fallback_inventory_asset_account(product, company=company)
                if not inv_acc:
                    raise ValueError(
                        f"Product '{product.name}' has no inventory_asset_account, and no fallback account was found."
                    )
                _add_line(je=je, account=inv_acc, debit=value, supplier=supplier)
                dr_total += value

                if qty > 0:
                    InventoryMovement.objects.create(
                        product=product,
                        company=company,
                        date=post_date,
                        qty_in=qty,
                        qty_out=DEC0,
                        unit_cost=unit_cost,
                        value=value,
                        location=stock_location,
                        source_type=source_type,
                        source_id=source_id,
                        gl_entry=je,
                        is_gl_posted=True,
                    )
                    _apply_stock_in(product, qty, unit_cost)
            else:
                # Non-inventory / Service: DR expense/COGS account
                exp_acc = _fallback_expense_account(product, company=company)
                if not exp_acc:
                    # skip line if no expense account found at all
                    continue
                _add_line(je=je, account=exp_acc, debit=amt, supplier=supplier)
                dr_total += amt

        if dr_total <= 0:
            je.delete()
            return  # nothing to post — silently exit

        # 3) CR Supplier A/P subaccount
        supplier_acc = _get_or_create_supplier_ap_subaccount(supplier, company=company)
        _add_line(je=je, account=supplier_acc, credit=dr_total, supplier=supplier)

        bill.journal_entry = je
        bill.is_posted = True
        bill.posted_at = timezone.now()
        bill._skip_inventory_signal = True
        bill.save(update_fields=["journal_entry", "is_posted", "posted_at"])


# Keep old name as alias for backward compat
post_bill_inventory = post_bill_to_gl


# -----------------------------
# Unified Expense Posting (GL + Inventory)
# -----------------------------
def post_expense_to_gl(expense):
    """
    EXPENSE posting (unified – handles ALL line types):

      Category lines:          DR category account
      Inventory item lines:    DR Inventory Asset + stock-in movement + weighted avg update
      Non-inventory item lines: DR Expense/COGS account
      Credit:                  CR expense.payment_account (cash/bank)

    Sets expense.journal_entry, is_posted, posted_at.
    """
    from expenses.models import ExpenseCategoryLine, ExpenseItemLine

    company = expense.company
    company_id = getattr(company, "id", company)
    source_type = "EXPENSE"
    source_id = expense.id
    post_date = expense.payment_date or timezone.localdate()
    supplier = expense.payee_supplier if getattr(expense, "payee_supplier_id", None) else None
    stock_location = resolve_location_from_doc(expense)

    total = _dec(expense.total_amount)
    if total <= 0:
        if expense.journal_entry_id:
            expense.journal_entry.delete()
            expense.journal_entry = None
            expense.is_posted = False
            expense.posted_at = None
            expense.save(update_fields=["journal_entry", "is_posted", "posted_at"])
        return

    if not expense.payment_account_id:
        raise ValueError("Expense must have a payment_account selected.")

    with transaction.atomic():
        _clear_inventory_movements(source_type, source_id)
        _delete_journal_entry_if_exists(expense)

        payee = ""
        if supplier:
            payee = getattr(supplier, "company_name", "") or ""
        elif getattr(expense, "payee_name", None):
            payee = expense.payee_name
        je = _create_journal_entry(
            date=post_date,
            description=f"Expense {getattr(expense, 'number_display', expense.id)}" + (f" – {payee}" if payee else ""),
            source_type=source_type,
            source_id=source_id,
        )
        if hasattr(je, "company_id"):
            je.company_id = company_id
            je.save(update_fields=["company"])

        dr_total = DEC0

        # 1) Category lines → DR category account
        for cl in ExpenseCategoryLine.objects.filter(expense=expense).select_related("category"):
            acc = cl.category
            amt = _dec(cl.amount)
            if acc and amt > 0:
                _add_line(je=je, account=acc, debit=amt, supplier=supplier)
                dr_total += amt

        # 2) Item lines
        for ln in ExpenseItemLine.objects.filter(expense=expense).select_related("product"):
            product = ln.product
            amt = _dec(ln.amount) if _dec(ln.amount) > 0 else (_dec(ln.qty) * _dec(ln.rate))
            amt = amt.quantize(_Q2, rounding=ROUND_HALF_UP)
            if amt <= 0:
                continue

            if product and getattr(product, 'track_inventory', False):
                qty = _dec(ln.qty)
                unit_cost = _dec(ln.rate)
                value = (qty * unit_cost if qty > 0 else amt).quantize(_Q2, rounding=ROUND_HALF_UP)

                inv_acc = _fallback_inventory_asset_account(product, company=company)
                if not inv_acc:
                    raise ValueError(
                        f"Product '{product.name}' has no inventory_asset_account, and no fallback account was found."
                    )
                _add_line(je=je, account=inv_acc, debit=value, supplier=supplier)
                dr_total += value

                if qty > 0:
                    InventoryMovement.objects.create(
                        product=product,
                        company=company,
                        date=post_date,
                        qty_in=qty,
                        qty_out=DEC0,
                        unit_cost=unit_cost,
                        value=value,
                        location=stock_location,
                        source_type=source_type,
                        source_id=source_id,
                        gl_entry=je,
                        is_gl_posted=True,
                    )
                    _apply_stock_in(product, qty, unit_cost)
            else:
                exp_acc = _fallback_expense_account(product, company=company)
                if not exp_acc:
                    continue
                _add_line(je=je, account=exp_acc, debit=amt, supplier=supplier)
                dr_total += amt

        if dr_total <= 0:
            je.delete()
            return  # nothing to post — silently exit

        # 3) CR payment account
        _add_line(je=je, account=expense.payment_account, credit=dr_total, supplier=supplier)

        expense.journal_entry = je
        expense.is_posted = True
        expense.posted_at = timezone.now()
        expense._skip_inventory_signal = True
        expense.save(update_fields=["journal_entry", "is_posted", "posted_at"])


# Keep old name as alias
post_expense_inventory = post_expense_to_gl


# -----------------------------
# Stock Out (Invoices)
# -----------------------------
def post_invoice_inventory_and_gl(invoice):
    """
    INVOICE:
      A) Revenue side:
          Dr A/R (customer.ar_account or fallback)
          Cr Income (product.income_account per line)
      B) COGS side for inventory products:
          Dr COGS (product.cogs_account OR product.expense_account OR fallback)
          Cr Inventory Asset (product.inventory_asset_account OR fallback)
      C) InventoryMovement qty_out using FIFO cost layers (one movement per FIFO layer consumed)
    """
    from sales.models import InvoiceItem
    from inventory.fifo import simulate_fifo_consumption, rebuild_layers_from_movements

    company = getattr(invoice, "company", None)
    source_type = "INVOICE"
    source_id = invoice.id
    post_date = (invoice.date_created.date() if invoice.date_created else timezone.localdate())
    customer = getattr(invoice, "customer", None)
    stock_location = resolve_location_from_doc(invoice)

    with transaction.atomic():
        _clear_inventory_movements(source_type, source_id)
        _delete_journal_entry_if_exists(invoice)

        je = _create_journal_entry(
            date=post_date,
            description=f"Invoice {invoice.id} posting",
            source_type=source_type,
            source_id=source_id,
        )
        company_id = getattr(company, "id", company) if company is not None else None
        if company_id:
            je.company_id = company_id
            je.save(update_fields=["company"])

        # A/R account (fallback safe)
        ar_acc = _fallback_ar_account(customer, company=company)
        if not ar_acc:
            raise ValueError("Missing Accounts Receivable account. Create one or set customer.ar_account.")

        total_ar = DEC0
        affected_products = set()

        lines = InvoiceItem.objects.filter(invoice=invoice).select_related("product")
        for ln in lines:
            product = ln.product
            if not product:
                continue

            qty = _dec(getattr(ln, "qty", None))
            unit_price = _dec(getattr(ln, "unit_price", None))

            if qty <= 0:
                continue

            line_sales = (qty * unit_price).quantize(_Q2, rounding=ROUND_HALF_UP)
            total_ar += line_sales

            income_acc = _fallback_sales_account(product, company=company)
            if not income_acc:
                raise ValueError(f"Product '{product.name}' missing income_account AND no fallback Sales/Revenue account exists.")

            _add_line(je=je, account=income_acc, debit=DEC0, credit=line_sales, customer=customer)

            # Inventory side
            if getattr(product, 'track_inventory', False):
                cogs_acc = _fallback_cogs_account(product, company=company)
                inv_acc = _fallback_inventory_asset_account(product, company=company)

                if not cogs_acc:
                    raise ValueError(f"Product '{product.name}' missing cogs_account/expense_account and no COGS fallback found.")
                if not inv_acc:
                    raise ValueError(f"Product '{product.name}' missing inventory_asset_account and no Inventory/Expense fallback found.")

                # Use FIFO simulation to get per-layer unit costs (read-only)
                fifo_rows = simulate_fifo_consumption(product, qty)
                for layer_cost, layer_qty in fifo_rows:
                    layer_value = (layer_qty * layer_cost).quantize(_Q2, rounding=ROUND_HALF_UP)
                    InventoryMovement.objects.create(
                        product=product,
                        company=company,
                        date=post_date,
                        qty_in=DEC0,
                        qty_out=layer_qty,
                        unit_cost=layer_cost,
                        value=layer_value,
                        location=stock_location,
                        source_type=source_type,
                        source_id=source_id,
                        gl_entry=je,
                        is_gl_posted=True,
                    )
                    _add_line(je=je, account=cogs_acc, debit=layer_value, credit=DEC0)
                    _add_line(je=je, account=inv_acc, debit=DEC0, credit=layer_value)

                affected_products.add(product)

        if total_ar > 0:
            _add_line(je=je, account=ar_acc, debit=total_ar, credit=DEC0, customer=customer)

        # Rebuild FIFO layers and update cached qty for all affected products
        for product in affected_products:
            from django.db.models import Sum as _Sum
            agg = product.movements.aggregate(tin=_Sum("qty_in"), tout=_Sum("qty_out"))
            product.quantity = _dec(agg["tin"]) - _dec(agg["tout"])
            product.save(update_fields=["quantity"])
            rebuild_layers_from_movements(product, company=company)

        invoice.journal_entry = je
        invoice.is_posted = True
        invoice.posted_at = timezone.now()
        invoice._skip_inventory_signal = True
        invoice.save(update_fields=["journal_entry", "is_posted", "posted_at"])

# --- Sales Receipt Posting ---
def post_sales_receipt_to_gl(receipt):
    """
    SALES RECEIPT posting (stock-out + COGS for inventory items):

      Creates InventoryMovement (qty_out) and updates product.quantity
      for each inventory-type line. The revenue/COGS GL entries are
      handled by sales/views.py _post_sales_receipt_to_ledger.
    """
    from sales.models import SalesReceiptLine

    company = receipt.company
    company_id = getattr(company, "id", company)
    source_type = "SALES_RECEIPT"
    source_id = receipt.id
    post_date = getattr(receipt, "receipt_date", None) or timezone.localdate()

    with transaction.atomic():
        _clear_inventory_movements(source_type, source_id)

        from inventory.fifo import simulate_fifo_consumption, rebuild_layers_from_movements
        affected_products = set()

        for ln in SalesReceiptLine.objects.filter(receipt=receipt).select_related("product"):
            product = ln.product
            if not product or not getattr(product, 'track_inventory', False):
                continue
            qty = _dec(ln.qty)
            if qty <= 0:
                continue

            fifo_rows = simulate_fifo_consumption(product, qty)
            for layer_cost, layer_qty in fifo_rows:
                layer_value = (layer_qty * layer_cost).quantize(_Q2, rounding=ROUND_HALF_UP)
                InventoryMovement.objects.create(
                    product=product,
                    company=company,
                    date=post_date,
                    qty_in=DEC0,
                    qty_out=layer_qty,
                    unit_cost=layer_cost,
                    value=layer_value,
                    source_type=source_type,
                    source_id=source_id,
                )

            affected_products.add(product)

        for product in affected_products:
            from django.db.models import Sum as _Sum
            agg = product.movements.aggregate(tin=_Sum("qty_in"), tout=_Sum("qty_out"))
            product.quantity = _dec(agg["tin"]) - _dec(agg["tout"])
            product.save(update_fields=["quantity"])
            rebuild_layers_from_movements(product, company=company)


# --- Assembly Build Completion ---
def complete_build(build, completed_by=None):
    """
    Complete an assembly build using the 2-step WIP accounting engine.

    Delegates to inventory.assembly_engine.complete_assembly which implements:
      Step 1: DR WIP / CR Raw Materials Inventory (per component, per FIFO layer)
      Step 2: DR Finished Goods / CR WIP

    Args:
        build: Build instance (status DRAFT, PENDING, or IN_PROGRESS).
        completed_by: Optional user performing the completion.
    """
    from inventory.assembly_engine import complete_assembly
    return complete_assembly(build, completed_by=completed_by)


# -----------------------------
# Opening Stock GL Posting
# -----------------------------
def _fallback_opening_equity_account(company=None):
    """
    Returns the Opening Balance Equity account for the company.

    Priority:
      1) Account named exactly "Opening Balance Equity"
      2) Account contains "Opening Balance"
      3) Account contains "Retained Earnings"
      4) Any equity-type account
    """
    company_id = getattr(company, "id", company) if company is not None else None

    for term in ("Opening Balance Equity", "Opening Balance", "Retained Earnings", "Equity"):
        qs = Account.objects.filter(is_active=True, account_name__icontains=term)
        if company_id:
            qs = qs.filter(company_id=company_id)
        acc = qs.first()
        if acc:
            return acc

    qs = Account.objects.filter(
        is_active=True,
        account_type__in=["OWNER_EQUITY"],
    )
    if company_id:
        qs = qs.filter(company_id=company_id)
    return qs.first()


def _fallback_adjustment_account(reason=None, company=None):
    """
    Returns an appropriate GL account for stock adjustment posting.

    Loss reasons (damage, theft, write_off, shrinkage, donation) use expense accounts.
    Other reasons use an inventory adjustment account.
    """
    LOSS_REASONS = {"damage", "theft", "write_off", "shrinkage", "donation"}
    company_id = getattr(company, "id", company) if company is not None else None

    for term in ("Inventory Adjustment", "Stock Adjustment", "Inventory Write",):
        qs = Account.objects.filter(is_active=True, account_name__icontains=term)
        if company_id:
            qs = qs.filter(company_id=company_id)
        acc = qs.first()
        if acc:
            return acc

    if reason in LOSS_REASONS:
        for term in ("Shrinkage", "Damage", "Loss", "Write", "COGS", "Cost of Goods"):
            qs = Account.objects.filter(is_active=True, account_name__icontains=term)
            if company_id:
                qs = qs.filter(company_id=company_id)
            acc = qs.first()
            if acc:
                return acc

    return _fallback_expense_account(None, company=company)


def post_opening_stock_to_gl(product, qty, unit_cost, date=None, company=None):
    """
    Post opening stock GL entry for a product:

      Dr Inventory Asset Account  (qty * unit_cost)
      Cr Opening Balance Equity   (qty * unit_cost)

    This is called when a product is assigned an opening stock balance
    at a cut-off date (e.g., when track_inventory is first enabled).

    Returns the created JournalEntry or None if GL accounts are missing.
    """
    company = company or getattr(product, "company", None)
    company_id = getattr(company, "id", company) if company is not None else None
    date = date or timezone.localdate()
    qty = _dec(qty)
    unit_cost = _dec(unit_cost)

    if qty <= DEC0 or unit_cost < DEC0:
        return None

    inv_acc = _fallback_inventory_asset_account(product, company=company)
    equity_acc = _fallback_opening_equity_account(company=company)

    if not inv_acc or not equity_acc:
        return None

    value = (qty * unit_cost).quantize(_Q2, rounding=ROUND_HALF_UP)
    source_type = "OPENING"
    source_id = product.id

    with transaction.atomic():
        # Delete any existing opening-stock GL entry for this product
        existing = JournalEntry.objects.filter(
            source_type=source_type,
            source_id=source_id,
        )
        if company_id:
            existing = existing.filter(company_id=company_id)
        existing.delete()

        je = JournalEntry.objects.create(
            date=date,
            description=f"Opening Stock – {product.name}",
            source_type=source_type,
            source_id=source_id,
        )
        if company_id:
            je.company_id = company_id
            je.save(update_fields=["company"])

        _add_line(je=je, account=inv_acc, debit=value)
        _add_line(je=je, account=equity_acc, credit=value)

        # Mark matching OPENING movements as GL-posted
        from inventory.models import InventoryMovement
        InventoryMovement.objects.filter(
            source_type=source_type,
            source_id=source_id,
        ).update(gl_entry=je, is_gl_posted=True)

        # Update product opening_stock_value for reporting
        product.opening_stock_value = value
        if not product.opening_stock_date:
            product.opening_stock_date = date
        product.save(update_fields=["opening_stock_value", "opening_stock_date"])

    return je


# -----------------------------
# Stock Adjustment GL Posting
# -----------------------------
def post_stock_adjustment_to_gl(adjustment):
    """
    Post GL entries for a StockAdjustment that has already been posted
    (adjustment.status == "posted").

    For each line:
      - qty_increase > 0:
          Dr Inventory Asset       (qty * unit_cost)
          Cr Inventory Adjustment  (expense/income)
      - qty_decrease > 0 (at FIFO cost):
          Dr Inventory Adjustment  (expense/income)
          Cr Inventory Asset       (FIFO cost)

    Creates InventoryMovements and links them to the JournalEntry.
    Sets adjustment.journal_entry, is_posted, posted_at.
    """
    from inventory.models import StockAdjustment, StockAdjustmentLine
    from inventory.fifo import simulate_fifo_consumption, rebuild_layers_from_movements
    from inventory.services import get_default_location

    if adjustment.status != "posted":
        raise ValueError("Only posted adjustments can be GL-posted.")

    company = getattr(adjustment, "company", None)
    company_id = getattr(company, "id", company) if company is not None else None
    source_type = "ADJUSTMENT"
    source_id = adjustment.id
    post_date = adjustment.date or timezone.localdate()
    adj_loc = get_default_location(company=company)

    with transaction.atomic():
        # Clear existing movements for this adjustment
        _clear_inventory_movements(source_type, source_id)
        _delete_journal_entry_if_exists(adjustment)

        je = _create_journal_entry(
            date=post_date,
            description=f"Stock Adjustment #{adjustment.id} – {adjustment.get_reason_display()}",
            source_type=source_type,
            source_id=source_id,
        )
        if company_id:
            je.company_id = company_id
            je.save(update_fields=["company"])

        adj_acc = _fallback_adjustment_account(reason=adjustment.reason, company=company)
        if not adj_acc:
            je.delete()
            raise ValueError(
                "No adjustment/expense account found. "
                "Please create an 'Inventory Adjustment' account in the Chart of Accounts."
            )

        total_debit = DEC0
        total_credit = DEC0
        affected_products = set()

        for line in StockAdjustmentLine.objects.filter(adjustment=adjustment).select_related("product"):
            product = line.product
            if not product or not getattr(product, "track_inventory", False):
                continue

            qty_increase = _dec(line.qty_increase)
            qty_decrease = _dec(line.qty_decrease)
            unit_cost = _dec(line.unit_cost)

            inv_acc = _fallback_inventory_asset_account(product, company=company)
            if not inv_acc:
                continue

            if qty_increase > DEC0:
                value = (qty_increase * unit_cost).quantize(_Q2, rounding=ROUND_HALF_UP)
                if value <= DEC0:
                    continue
                mv = InventoryMovement.objects.create(
                    product=product,
                    company=company,
                    date=post_date,
                    qty_in=qty_increase,
                    qty_out=DEC0,
                    unit_cost=unit_cost,
                    value=value,
                    location=adj_loc,
                    source_type=source_type,
                    source_id=source_id,
                    gl_entry=je,
                    is_gl_posted=True,
                )
                # Dr Inventory Asset (increase)
                _add_line(je=je, account=inv_acc, debit=value)
                total_debit += value
                # Cr Adjustment Account
                _add_line(je=je, account=adj_acc, credit=value)
                total_credit += value
                affected_products.add(product)

            elif qty_decrease > DEC0:
                fifo_rows = simulate_fifo_consumption(product, qty_decrease)
                if not fifo_rows:
                    continue
                for layer_cost, layer_qty in fifo_rows:
                    layer_value = (layer_qty * layer_cost).quantize(_Q2, rounding=ROUND_HALF_UP)
                    if layer_value <= DEC0:
                        continue
                    InventoryMovement.objects.create(
                        product=product,
                        company=company,
                        date=post_date,
                        qty_in=DEC0,
                        qty_out=layer_qty,
                        unit_cost=layer_cost,
                        value=layer_value,
                        location=adj_loc,
                        source_type=source_type,
                        source_id=source_id,
                        gl_entry=je,
                        is_gl_posted=True,
                    )
                    # Dr Adjustment Account (loss/write-off)
                    _add_line(je=je, account=adj_acc, debit=layer_value)
                    total_debit += layer_value
                    # Cr Inventory Asset
                    _add_line(je=je, account=inv_acc, credit=layer_value)
                    total_credit += layer_value
                affected_products.add(product)

        if total_debit <= DEC0:
            je.delete()
            return

        # Rebuild FIFO layers and update cached qty for affected products
        for product in affected_products:
            from django.db.models import Sum as _Sum
            agg = product.movements.aggregate(tin=_Sum("qty_in"), tout=_Sum("qty_out"))
            product.quantity = _dec(agg["tin"]) - _dec(agg["tout"])
            product.save(update_fields=["quantity"])
            rebuild_layers_from_movements(product, company=company)

        adjustment.journal_entry = je
        adjustment.save(update_fields=["journal_entry"])
