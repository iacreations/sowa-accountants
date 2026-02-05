# inventory/accounting.py
from collections import defaultdict
from django.db import models
from decimal import Decimal
from django.db import transaction
from django.utils import timezone

from accounts.models import JournalEntry, JournalLine, Account
from inventory.models import InventoryMovement, Product

DEC0 = Decimal("0.00")


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
        debit=_dec(debit),
        credit=_dec(credit),
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


def _find_account_by_name_contains(text: str):
    return Account.objects.filter(is_active=True, account_name__icontains=text).order_by("account_name").first()


def _fallback_inventory_asset_account(product: Product):
    """
    QuickBooks-style fallback when the product has no inventory_asset_account set.

    Priority:
      1) product.inventory_asset_account (if set)
      2) Any active account with name like Inventory/Stock/Merchandise
      3) If still none, fallback to product.expense_account (your request: some inventory may fall under expenses)
      4) Any active expense-like account by name "Cost of Sales"/"Expenses"/"Expense"
      5) None (caller decides what to do)
    """
    inv_acc = getattr(product, "inventory_asset_account", None)
    if inv_acc:
        return inv_acc

    inv_acc = _find_account_by_name_contains("Inventory") or _find_account_by_name_contains("Stock") or _find_account_by_name_contains("Merchandise")
    if inv_acc:
        return inv_acc

    exp_acc = getattr(product, "expense_account", None)
    if exp_acc:
        return exp_acc

    exp_acc = _find_account_by_name_contains("Cost of Sales") or _find_account_by_name_contains("Cost of Goods") or _find_account_by_name_contains("COGS") \
              or _find_account_by_name_contains("Expenses") or _find_account_by_name_contains("Expense")
    return exp_acc


def _fallback_cogs_account(product: Product):
    """
    Fallback COGS for Inventory sales posting.

    Priority:
      1) product.cogs_account (if set)
      2) product.expense_account (allowed in your 3-level COA setup)
      3) Any active account by name "Cost of Sales"/"Cost of Goods"/"COGS"
      4) Any "Expenses"/"Expense"
    """
    cogs = getattr(product, "cogs_account", None)
    if cogs:
        return cogs

    exp = getattr(product, "expense_account", None)
    if exp:
        return exp

    cogs = _find_account_by_name_contains("Cost of Sales") or _find_account_by_name_contains("Cost of Goods") or _find_account_by_name_contains("COGS")
    if cogs:
        return cogs

    return _find_account_by_name_contains("Expenses") or _find_account_by_name_contains("Expense")


def _fallback_ap_account(supplier):
    """
    AP priority:
      1) supplier.ap_account (if exists)
      2) CoA account named exactly "Accounts Payable"
      3) CoA contains "Accounts Payable" / "Payable"
    """
    if supplier and getattr(supplier, "ap_account_id", None):
        return supplier.ap_account

    ap = Account.objects.filter(account_name__iexact="Accounts Payable", is_active=True).first()
    if ap:
        return ap

    return _find_account_by_name_contains("Accounts Payable") or _find_account_by_name_contains("Payable")


def _fallback_ar_account(customer):
    """
    AR priority:
      1) customer.ar_account (if exists)
      2) CoA account named exactly "Accounts Receivable"
      3) CoA contains "Accounts Receivable" / "Receivable"
    """
    if customer and getattr(customer, "ar_account_id", None):
        return customer.ar_account

    ar = Account.objects.filter(account_name__iexact="Accounts Receivable", is_active=True).first()
    if ar:
        return ar

    return _find_account_by_name_contains("Accounts Receivable") or _find_account_by_name_contains("Receivable")


def _fallback_sales_account(product: Product):
    """
    Income fallback:
      1) product.income_account
      2) "Sales"
      3) account contains "Sales"/"Revenue"
    """
    inc = getattr(product, "income_account", None)
    if inc:
        return inc

    sales = Account.objects.filter(account_name__iexact="Sales", is_active=True).first()
    if sales:
        return sales

    return _find_account_by_name_contains("Sales") or _find_account_by_name_contains("Revenue")


# -----------------------------
# Stock In (Bills/Expenses)
# -----------------------------
def _apply_stock_in(product: Product, qty_in: Decimal, unit_cost: Decimal):
    """
    Weighted average:
      new_avg = (old_qty*old_avg + in_qty*in_cost) / (old_qty + in_qty)
    """
    old_qty = _dec(product.quantity)
    old_avg = _dec(product.avg_cost)

    new_qty = old_qty + qty_in
    if new_qty <= 0:
        product.quantity = DEC0
        product.avg_cost = DEC0
    else:
        new_avg = ((old_qty * old_avg) + (qty_in * unit_cost)) / new_qty
        product.quantity = new_qty
        product.avg_cost = new_avg

    product.save(update_fields=["quantity", "avg_cost"])


def post_bill_inventory(bill):
    """
    BILL (Inventory Part):
      - Stock IN for Inventory items
      - GL:
          Dr Inventory Asset (or fallback allowed to Expense if you want)
          Cr A/P (supplier.ap_account or fallback)

    ✅ FIXED: No longer crashes if product.inventory_asset_account is missing.
             Uses a safe fallback so you can keep testing bills.
    """
    from expenses.models import BillItemLine  # local import to avoid circular

    source_type = "BILL"
    source_id = bill.id
    post_date = bill.bill_date or timezone.localdate()
    supplier = bill.supplier if bill.supplier_id else None

    with transaction.atomic():
        _clear_inventory_movements(source_type, source_id)
        _delete_journal_entry_if_exists(bill)

        je = _create_journal_entry(
            date=post_date,
            description=f"Bill {bill.bill_no} inventory posting",
            source_type=source_type,
            source_id=source_id,
        )

        total_value = DEC0

        lines = BillItemLine.objects.filter(bill=bill).select_related("product")
        for ln in lines:
            product = ln.product
            if not product or product.type != "Inventory":
                continue

            qty = _dec(ln.qty)
            unit_cost = _dec(ln.rate)

            # ignore bad lines
            if qty <= 0:
                continue

            value = qty * unit_cost

            InventoryMovement.objects.create(
                product=product,
                date=post_date,
                qty_in=qty,
                qty_out=DEC0,
                unit_cost=unit_cost,
                value=value,
                source_type=source_type,
                source_id=source_id,
            )

            _apply_stock_in(product, qty, unit_cost)
            total_value += value

            # ✅ fallback inventory account
            inv_acc = _fallback_inventory_asset_account(product)
            if not inv_acc:
                raise ValueError(
                    f"Product '{product.name}' has no inventory_asset_account, and no fallback Inventory/Stock/Expense account was found."
                )

            _add_line(je=je, account=inv_acc, debit=value, credit=DEC0, supplier=supplier)

        # credit AP once (total)
        if total_value > 0:
            ap_acc = _fallback_ap_account(supplier)
            if not ap_acc:
                raise ValueError("Missing Accounts Payable account. Create one or set supplier.ap_account.")

            _add_line(je=je, account=ap_acc, debit=DEC0, credit=total_value, supplier=supplier)

        bill.journal_entry = je
        bill.is_posted = True
        bill.posted_at = timezone.now()
        bill.save(update_fields=["journal_entry", "is_posted", "posted_at"])


def post_expense_inventory(expense):
    """
    EXPENSE (cash/bank purchase):
      - Stock IN for Inventory items
      - GL:
          Dr Inventory Asset (or fallback)
          Cr expense.payment_account (cash/bank)

    ✅ FIXED: No longer crashes if product.inventory_asset_account is missing.
    """
    from expenses.models import ExpenseItemLine

    source_type = "EXPENSE"
    source_id = expense.id
    post_date = expense.payment_date or timezone.localdate()
    supplier = expense.payee_supplier if getattr(expense, "payee_supplier_id", None) else None

    with transaction.atomic():
        _clear_inventory_movements(source_type, source_id)
        _delete_journal_entry_if_exists(expense)

        je = _create_journal_entry(
            date=post_date,
            description=f"Expense {expense.number_display} inventory posting",
            source_type=source_type,
            source_id=source_id,
        )

        total_value = DEC0
        lines = ExpenseItemLine.objects.filter(expense=expense).select_related("product")
        for ln in lines:
            product = ln.product
            if not product or product.type != "Inventory":
                continue

            qty = _dec(ln.qty)
            unit_cost = _dec(ln.rate)

            if qty <= 0:
                continue

            value = qty * unit_cost

            InventoryMovement.objects.create(
                product=product,
                date=post_date,
                qty_in=qty,
                qty_out=DEC0,
                unit_cost=unit_cost,
                value=value,
                source_type=source_type,
                source_id=source_id,
            )

            _apply_stock_in(product, qty, unit_cost)
            total_value += value

            inv_acc = _fallback_inventory_asset_account(product)
            if not inv_acc:
                raise ValueError(
                    f"Product '{product.name}' has no inventory_asset_account, and no fallback Inventory/Stock/Expense account was found."
                )

            _add_line(je=je, account=inv_acc, debit=value, credit=DEC0, supplier=supplier)

        if total_value > 0:
            pay_acc = getattr(expense, "payment_account", None)
            if not pay_acc:
                raise ValueError("Expense missing payment_account (cash/bank).")
            _add_line(je=je, account=pay_acc, debit=DEC0, credit=total_value, supplier=supplier)

        expense.journal_entry = je
        expense.is_posted = True
        expense.posted_at = timezone.now()
        expense.save(update_fields=["journal_entry", "is_posted", "posted_at"])


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
      C) InventoryMovement qty_out using product.avg_cost
    """
    from sales.models import InvoiceItem

    source_type = "INVOICE"
    source_id = invoice.id
    post_date = (invoice.date_created.date() if invoice.date_created else timezone.localdate())
    customer = getattr(invoice, "customer", None)

    with transaction.atomic():
        _clear_inventory_movements(source_type, source_id)
        _delete_journal_entry_if_exists(invoice)

        je = _create_journal_entry(
            date=post_date,
            description=f"Invoice {invoice.id} posting",
            source_type=source_type,
            source_id=source_id,
        )

        # A/R account (fallback safe)
        ar_acc = _fallback_ar_account(customer)
        if not ar_acc:
            raise ValueError("Missing Accounts Receivable account. Create one or set customer.ar_account.")

        total_ar = DEC0

        lines = InvoiceItem.objects.filter(invoice=invoice).select_related("product")
        for ln in lines:
            product = ln.product
            if not product:
                continue

            qty = _dec(getattr(ln, "qty", None))
            unit_price = _dec(getattr(ln, "unit_price", None))

            if qty <= 0:
                continue

            line_sales = qty * unit_price
            total_ar += line_sales

            income_acc = _fallback_sales_account(product)
            if not income_acc:
                raise ValueError(f"Product '{product.name}' missing income_account AND no fallback Sales/Revenue account exists.")

            _add_line(je=je, account=income_acc, debit=DEC0, credit=line_sales, customer=customer)

            # Inventory side
            if product.type == "Inventory":
                cogs_acc = _fallback_cogs_account(product)
                inv_acc = _fallback_inventory_asset_account(product)

                if not cogs_acc:
                    raise ValueError(f"Product '{product.name}' missing cogs_account/expense_account and no COGS fallback found.")
                if not inv_acc:
                    raise ValueError(f"Product '{product.name}' missing inventory_asset_account and no Inventory/Expense fallback found.")

                unit_cost = _dec(getattr(product, "avg_cost", None))
                cogs_value = qty * unit_cost

                InventoryMovement.objects.create(
                    product=product,
                    date=post_date,
                    qty_in=DEC0,
                    qty_out=qty,
                    unit_cost=unit_cost,
                    value=cogs_value,
                    source_type=source_type,
                    source_id=source_id,
                )

                # update cached qty (avg_cost maintained on purchases)
                product.quantity = _dec(product.quantity) - qty
                product.save(update_fields=["quantity"])

                _add_line(je=je, account=cogs_acc, debit=cogs_value, credit=DEC0)
                _add_line(je=je, account=inv_acc, debit=DEC0, credit=cogs_value)

        if total_ar > 0:
            _add_line(je=je, account=ar_acc, debit=total_ar, credit=DEC0, customer=customer)

        invoice.journal_entry = je
        invoice.is_posted = True
        invoice.posted_at = timezone.now()
        invoice.save(update_fields=["journal_entry", "is_posted", "posted_at"])

# expenses

def _find_fallback_expense_account():
    # safest fallback: any Expense-type account if you have a naming convention
    acc = Account.objects.filter(is_active=True, account_name__icontains="Expense").first()
    if acc:
        return acc
    # last resort: first active account
    return Account.objects.filter(is_active=True).first()

def _post_expense_to_ledger(expense):
    """
    EXPENSE posting:

      DR  expense accounts (category lines + item non-inventory/service lines)
      DR  inventory asset (for inventory item lines)
      CR  payment_account (bank/cash/mobile money/etc)

    Uses expense.journal_entry so edits UPDATE same JournalEntry (no duplicates).
    """

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

    # Collect DR totals per account
    dr_by_account = defaultdict(lambda: DEC0)

    # 1) Category lines => DR selected account
    for cl in expense.cat_lines.select_related("category"):
        acc = cl.category
        amt = _dec(cl.amount)
        if acc and amt > 0:
            dr_by_account[acc] += amt

    # 2) Item lines
    fallback_exp = _find_fallback_expense_account()

    for il in expense.item_lines.select_related("product"):
        amt = _dec(il.amount)
        if amt <= 0:
            continue

        p = il.product
        if p and p.type == "Inventory":
            # Inventory purchases should hit inventory asset (QuickBooks standard)
            inv_acc = getattr(p, "inventory_asset_account", None)
            if not inv_acc:
                # If you haven't configured it, fallback to expense_account so user isn't blocked
                inv_acc = getattr(p, "expense_account", None) or fallback_exp
            if inv_acc:
                dr_by_account[inv_acc] += amt
        else:
            # Non-inventory/service purchase => expense account
            exp_acc = None
            if p:
                exp_acc = getattr(p, "expense_account", None)
            if not exp_acc:
                exp_acc = fallback_exp
            if exp_acc:
                dr_by_account[exp_acc] += amt

    dr_total = sum(dr_by_account.values()) or DEC0
    if dr_total <= 0:
        return

    # Create/update JournalEntry
    entry_date = expense.payment_date or timezone.localdate()
    payee = expense.payee_supplier.company_name if expense.payee_supplier_id else (expense.payee_name or "")
    desc = f"Expense {expense.number_display}" + (f" – {payee}" if payee else "")

    entry = expense.journal_entry
    if not entry:
        entry = JournalEntry.objects.create(
            date=entry_date,
            description=desc,
            source_type="expense",
            source_id=expense.id,
        )
        expense.journal_entry = entry
        expense.save(update_fields=["journal_entry"])
    else:
        entry.date = entry_date
        entry.description = desc
        entry.source_type = "expense"
        entry.source_id = expense.id
        entry.save(update_fields=["date", "description", "source_type", "source_id"])

    # Replace JE lines
    JournalLine.objects.filter(entry=entry).delete()

    # DR lines
    for acc, amt in dr_by_account.items():
        if amt > 0:
            JournalLine.objects.create(
                entry=entry,
                account=acc,
                debit=amt,
                credit=DEC0,
                supplier=expense.payee_supplier if expense.payee_supplier_id else None,
            )

    # CR payment account
    JournalLine.objects.create(
        entry=entry,
        account=expense.payment_account,
        debit=DEC0,
        credit=dr_total,
        supplier=expense.payee_supplier if expense.payee_supplier_id else None,
    )

    # mark posted
    expense.is_posted = True
    expense.posted_at = timezone.now()
    expense.save(update_fields=["is_posted", "posted_at"])
