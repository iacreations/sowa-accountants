# inventory/signals.py
#
# NOTE: Bill and Expense GL posting is now handled by unified functions
# called directly from the views (post_bill_to_gl, post_expense_to_gl).
# Signal-based posting is DISABLED to prevent double journal entries.
#
from django.db.models.signals import post_save
from django.dispatch import receiver

from expenses.models import Bill, Expense
from sales.models import Newinvoice

from inventory.accounting import (
    post_bill_to_gl as post_bill_inventory,
    post_expense_to_gl as post_expense_inventory,
    post_invoice_inventory_and_gl,
)


# Disabled: views now call post_bill_to_gl() directly
# @receiver(post_save, sender=Bill)
# def bill_posting(sender, instance: Bill, created, **kwargs):
#     if instance.is_posted:
#         return
#     if instance.item_lines.exists():
#         post_bill_inventory(instance)


# Disabled: views now call post_expense_to_gl() directly
# @receiver(post_save, sender=Expense)
# def expense_posting(sender, instance: Expense, created, **kwargs):
#     if instance.is_posted:
#         return
#     if instance.item_lines.exists():
#         post_expense_inventory(instance)

