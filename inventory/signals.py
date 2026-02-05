# inventory/signals.py
from django.db.models.signals import post_save
from django.dispatch import receiver

from expenses.models import Bill, Expense
from sales.models import Newinvoice

from inventory.accounting import (
    post_bill_inventory,
    post_expense_inventory,
    post_invoice_inventory_and_gl,
)


@receiver(post_save, sender=Bill)
def bill_posting(sender, instance: Bill, created, **kwargs):
    # Auto post only if not posted yet
    if instance.is_posted:
        return
    # Only post if it has item lines (otherwise do nothing)
    if instance.item_lines.exists():
        post_bill_inventory(instance)


@receiver(post_save, sender=Expense)
def expense_posting(sender, instance: Expense, created, **kwargs):
    if instance.is_posted:
        return
    if instance.item_lines.exists():
        post_expense_inventory(instance)

