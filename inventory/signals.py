# inventory/signals.py
"""
Post-save signals for automatic inventory movement generation.
Each document type triggers movement rebuilding when posted.
"""
from django.db.models.signals import post_save, pre_delete
from django.dispatch import receiver
from django.db import transaction


# ------------------------------------------------------------------
# Bill signals - Stock IN
# ------------------------------------------------------------------
def connect_bill_signals():
    try:
        from expenses.models import Bill
        from inventory.services import rebuild_movements_for_bill

        @receiver(post_save, sender=Bill, weak=False)
        def bill_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if getattr(instance, 'is_posted', False) or getattr(instance, 'status', '') == 'posted':
                try:
                    with transaction.atomic():
                        rebuild_movements_for_bill(instance)
                except Exception:
                    pass

        @receiver(pre_delete, sender=Bill, weak=False)
        def bill_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("BILL", instance.id, company=company)

    except ImportError:
        pass


# ------------------------------------------------------------------
# Expense signals - Stock IN
# ------------------------------------------------------------------
def connect_expense_signals():
    try:
        from expenses.models import Expense
        from inventory.services import rebuild_movements_for_expense

        @receiver(post_save, sender=Expense, weak=False)
        def expense_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if getattr(instance, 'is_posted', False) or getattr(instance, 'status', '') == 'posted':
                try:
                    with transaction.atomic():
                        rebuild_movements_for_expense(instance)
                except Exception:
                    pass

        @receiver(pre_delete, sender=Expense, weak=False)
        def expense_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("EXPENSE", instance.id, company=company)

    except ImportError:
        pass


# ------------------------------------------------------------------
# StockAdjustment signals
# ------------------------------------------------------------------
def connect_adjustment_signals():
    try:
        from inventory.models import StockAdjustment
        from inventory.services import rebuild_movements_for_stock_adjustment, _delete_existing_source_movements

        @receiver(post_save, sender=StockAdjustment, weak=False)
        def adjustment_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if instance.status == 'posted':
                try:
                    with transaction.atomic():
                        rebuild_movements_for_stock_adjustment(instance)
                except Exception:
                    pass
            elif instance.status == 'void':
                try:
                    company = getattr(instance, 'company', None)
                    _delete_existing_source_movements("ADJUSTMENT", instance.id, company=company)
                except Exception:
                    pass

    except ImportError:
        pass


# ------------------------------------------------------------------
# Cheque signals - Stock IN
# ------------------------------------------------------------------
def connect_cheque_signals():
    try:
        from expenses.models import Cheque
        from inventory.services import rebuild_movements_for_cheque

        @receiver(post_save, sender=Cheque, weak=False)
        def cheque_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            try:
                with transaction.atomic():
                    rebuild_movements_for_cheque(instance)
            except Exception:
                pass

        @receiver(pre_delete, sender=Cheque, weak=False)
        def cheque_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("CHEQUE", instance.id, company=company)

    except ImportError:
        pass


# ------------------------------------------------------------------
# Invoice signals - Stock OUT using FIFO
# ------------------------------------------------------------------
def connect_invoice_signals():
    try:
        from sales.models import Newinvoice
        from inventory.services import rebuild_movements_for_invoice

        @receiver(post_save, sender=Newinvoice, weak=False)
        def invoice_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if getattr(instance, 'is_posted', False):
                try:
                    with transaction.atomic():
                        rebuild_movements_for_invoice(instance)
                except Exception:
                    pass

        @receiver(pre_delete, sender=Newinvoice, weak=False)
        def invoice_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("INVOICE", instance.id, company=company)

    except ImportError:
        pass


# ------------------------------------------------------------------
# SalesReceipt signals - Stock OUT using FIFO
# ------------------------------------------------------------------
def connect_sales_receipt_signals():
    try:
        from sales.models import SalesReceipt
        from inventory.services import rebuild_movements_for_sales_receipt

        @receiver(post_save, sender=SalesReceipt, weak=False)
        def sales_receipt_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            if getattr(instance, 'is_posted', False):
                try:
                    with transaction.atomic():
                        rebuild_movements_for_sales_receipt(instance)
                except Exception:
                    pass

        @receiver(pre_delete, sender=SalesReceipt, weak=False)
        def sales_receipt_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("SALES_RECEIPT", instance.id, company=company)

    except ImportError:
        pass


# ------------------------------------------------------------------
# StockTransfer signals - OUT from source location, IN to destination
# ------------------------------------------------------------------
def connect_stock_transfer_signals():
    try:
        from inventory.models import StockTransfer
        from inventory.services import rebuild_movements_for_stock_transfer

        @receiver(post_save, sender=StockTransfer, weak=False)
        def stock_transfer_inventory_signal(sender, instance, created, **kwargs):
            if getattr(instance, '_skip_inventory_signal', False):
                return
            try:
                with transaction.atomic():
                    rebuild_movements_for_stock_transfer(instance)
            except Exception:
                pass

        @receiver(pre_delete, sender=StockTransfer, weak=False)
        def stock_transfer_delete_signal(sender, instance, **kwargs):
            from inventory.services import _delete_existing_source_movements
            company = getattr(instance, 'company', None)
            _delete_existing_source_movements("TRANSFER", instance.id, company=company)

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
