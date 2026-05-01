# inventory/signals.py
"""
Post-save signals for automatic inventory movement generation.
Each document type triggers movement rebuilding when posted.

ERROR HANDLING POLICY
─────────────────────
All signal handlers must use explicit logging rather than silently swallowing
exceptions with ``except Exception: pass``.  Accounting errors are never safe
to ignore — an unhandled failure here means movements or GL entries are missing
from the audit trail.

Rule: log the error with full context, then re-raise so the transaction rolls
back and the caller sees the failure.
"""
import logging
from django.db.models.signals import post_save, pre_delete
from django.db import transaction
from inventory.exceptions import InventoryPostingError

logger = logging.getLogger("inventory.signals")


# ------------------------------------------------------------------
# Bill signals - Stock IN
# ------------------------------------------------------------------
def connect_bill_signals():
    try:
        from expenses.models import Bill
        from inventory.services import rebuild_movements_for_bill

        def bill_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if getattr(instance, 'is_posted', False) or getattr(instance, 'status', '') == 'posted':
                try:
                    with transaction.atomic():
                        rebuild_movements_for_bill(instance)
                except InventoryPostingError:
                    logger.exception(
                        "InventoryPostingError rebuilding movements for Bill #%s",
                        instance.id,
                    )
                    raise
                except Exception:
                    logger.exception(
                        "Unexpected error rebuilding inventory movements for Bill #%s. "
                        "Inventory and GL may be out of sync — review this bill.",
                        instance.id,
                    )
                    raise

        def bill_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("BILL", instance.id, company=company)

        post_save.connect(bill_inventory_signal, sender=Bill, weak=False,
                          dispatch_uid="inventory.bill_post_save")
        pre_delete.connect(bill_delete_signal, sender=Bill, weak=False,
                           dispatch_uid="inventory.bill_pre_delete")

    except ImportError:
        pass


# ------------------------------------------------------------------
# Expense signals - Stock IN
# ------------------------------------------------------------------
def connect_expense_signals():
    try:
        from expenses.models import Expense
        from inventory.services import rebuild_movements_for_expense

        def expense_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if getattr(instance, 'is_posted', False) or getattr(instance, 'status', '') == 'posted':
                try:
                    with transaction.atomic():
                        rebuild_movements_for_expense(instance)
                except InventoryPostingError:
                    logger.exception(
                        "InventoryPostingError rebuilding movements for Expense #%s",
                        instance.id,
                    )
                    raise
                except Exception:
                    logger.exception(
                        "Unexpected error rebuilding inventory movements for Expense #%s. "
                        "Inventory and GL may be out of sync — review this expense.",
                        instance.id,
                    )
                    raise

        def expense_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("EXPENSE", instance.id, company=company)

        post_save.connect(expense_inventory_signal, sender=Expense, weak=False,
                          dispatch_uid="inventory.expense_post_save")
        pre_delete.connect(expense_delete_signal, sender=Expense, weak=False,
                           dispatch_uid="inventory.expense_pre_delete")

    except ImportError:
        pass


# ------------------------------------------------------------------
# StockAdjustment signals
# ------------------------------------------------------------------
def connect_adjustment_signals():
    try:
        from inventory.models import StockAdjustment
        from inventory.services import rebuild_movements_for_stock_adjustment, _delete_existing_source_movements

        def adjustment_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if instance.status == 'posted':
                try:
                    with transaction.atomic():
                        rebuild_movements_for_stock_adjustment(instance)
                except InventoryPostingError:
                    logger.exception(
                        "InventoryPostingError rebuilding movements for StockAdjustment #%s",
                        instance.id,
                    )
                    raise
                except Exception:
                    logger.exception(
                        "Unexpected error rebuilding inventory movements for StockAdjustment #%s. "
                        "Inventory and GL may be out of sync — review this adjustment.",
                        instance.id,
                    )
                    raise
            elif instance.status == 'void':
                try:
                    company = getattr(instance, 'company', None)
                    _delete_existing_source_movements("ADJUSTMENT", instance.id, company=company)
                except Exception:
                    logger.exception(
                        "Error deleting movements for voided StockAdjustment #%s",
                        instance.id,
                    )
                    raise

        post_save.connect(adjustment_inventory_signal, sender=StockAdjustment, weak=False,
                          dispatch_uid="inventory.adjustment_post_save")

    except ImportError:
        pass


# ------------------------------------------------------------------
# Cheque signals - Stock IN
# ------------------------------------------------------------------
def connect_cheque_signals():
    try:
        from expenses.models import Cheque
        from inventory.services import rebuild_movements_for_cheque

        def cheque_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            try:
                with transaction.atomic():
                    rebuild_movements_for_cheque(instance)
            except InventoryPostingError:
                logger.exception(
                    "InventoryPostingError rebuilding movements for Cheque #%s",
                    instance.id,
                )
                raise
            except Exception:
                logger.exception(
                    "Unexpected error rebuilding inventory movements for Cheque #%s. "
                    "Inventory and GL may be out of sync — review this cheque.",
                    instance.id,
                )
                raise

        def cheque_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("CHEQUE", instance.id, company=company)

        post_save.connect(cheque_inventory_signal, sender=Cheque, weak=False,
                          dispatch_uid="inventory.cheque_post_save")
        pre_delete.connect(cheque_delete_signal, sender=Cheque, weak=False,
                           dispatch_uid="inventory.cheque_pre_delete")

    except ImportError:
        pass


# ------------------------------------------------------------------
# Invoice signals - Stock OUT using FIFO
#
# IMPORTANT: COGS and inventory GL posting for invoices is handled
# exclusively by inventory.accounting.post_invoice_inventory_and_gl().
# That function sets invoice._skip_inventory_signal = True before
# saving, so this signal will NOT run after a normal posting.
#
# This signal fires only when an invoice is saved as is_posted=True
# without going through post_invoice_inventory_and_gl (e.g. data
# migration or admin edits).  In that edge case it calls
# post_invoice_inventory_and_gl to ensure movements are created.
# ------------------------------------------------------------------
def connect_invoice_signals():
    try:
        from sales.models import Newinvoice

        def invoice_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                # post_invoice_inventory_and_gl() already handled this.
                return
            if getattr(instance, 'is_posted', False):
                try:
                    from inventory.accounting import post_invoice_inventory_and_gl
                    with transaction.atomic():
                        post_invoice_inventory_and_gl(instance)
                except InventoryPostingError:
                    logger.exception(
                        "InventoryPostingError in invoice signal for Invoice #%s. "
                        "COGS/inventory GL posting failed — review this invoice.",
                        instance.id,
                    )
                    raise
                except Exception:
                    logger.exception(
                        "Unexpected error in invoice signal for Invoice #%s. "
                        "COGS/inventory GL posting may be incomplete.",
                        instance.id,
                    )
                    raise

        def invoice_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("INVOICE", instance.id, company=company)

        post_save.connect(invoice_inventory_signal, sender=Newinvoice, weak=False,
                          dispatch_uid="inventory.invoice_post_save")
        pre_delete.connect(invoice_delete_signal, sender=Newinvoice, weak=False,
                           dispatch_uid="inventory.invoice_pre_delete")

    except ImportError:
        pass


# ------------------------------------------------------------------
# SalesReceipt signals - Stock OUT using FIFO
# ------------------------------------------------------------------
def connect_sales_receipt_signals():
    try:
        from sales.models import SalesReceipt
        from inventory.services import rebuild_movements_for_sales_receipt

        def sales_receipt_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if getattr(instance, 'is_posted', False):
                try:
                    with transaction.atomic():
                        rebuild_movements_for_sales_receipt(instance)
                except InventoryPostingError:
                    logger.exception(
                        "InventoryPostingError rebuilding movements for SalesReceipt #%s",
                        instance.id,
                    )
                    raise
                except Exception:
                    logger.exception(
                        "Unexpected error rebuilding inventory movements for SalesReceipt #%s. "
                        "Inventory and GL may be out of sync — review this receipt.",
                        instance.id,
                    )
                    raise

        def sales_receipt_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("SALES_RECEIPT", instance.id, company=company)

        post_save.connect(sales_receipt_inventory_signal, sender=SalesReceipt, weak=False,
                          dispatch_uid="inventory.sales_receipt_post_save")
        pre_delete.connect(sales_receipt_delete_signal, sender=SalesReceipt, weak=False,
                           dispatch_uid="inventory.sales_receipt_pre_delete")

    except ImportError:
        pass


# ------------------------------------------------------------------
# StockTransfer signals - OUT from source, IN to destination
#
# Transfers do NOT create GL entries.  They only create inventory
# movements to track stock movement between locations.  FIFO layers
# are rebuilt at the destination (TRANSFER IN) so that company-wide
# inventory value is preserved.
# ------------------------------------------------------------------
def connect_stock_transfer_signals():
    try:
        from inventory.models import StockTransfer
        from inventory.services import rebuild_movements_for_stock_transfer

        def stock_transfer_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            try:
                with transaction.atomic():
                    rebuild_movements_for_stock_transfer(instance)
            except InventoryPostingError:
                logger.exception(
                    "InventoryPostingError rebuilding movements for StockTransfer #%s",
                    instance.id,
                )
                raise
            except Exception:
                logger.exception(
                    "Unexpected error rebuilding inventory movements for StockTransfer #%s. "
                    "Location quantities may be incorrect — review this transfer.",
                    instance.id,
                )
                raise

        def stock_transfer_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("TRANSFER", instance.id, company=company)

        post_save.connect(stock_transfer_inventory_signal, sender=StockTransfer, weak=False,
                          dispatch_uid="inventory.stock_transfer_post_save")
        pre_delete.connect(stock_transfer_delete_signal, sender=StockTransfer, weak=False,
                           dispatch_uid="inventory.stock_transfer_pre_delete")

    except ImportError:
        pass


# ------------------------------------------------------------------
# Connect all signals
# ------------------------------------------------------------------
connect_bill_signals()
connect_expense_signals()
connect_cheque_signals()
connect_invoice_signals()
connect_sales_receipt_signals()
connect_adjustment_signals()
connect_stock_transfer_signals()

