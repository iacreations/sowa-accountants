# sales/signals.py
"""
Register inventory signals for sales documents (Invoice, SalesReceipt).
"""
from inventory.signals import connect_invoice_signals, connect_sales_receipt_signals

connect_invoice_signals()
connect_sales_receipt_signals()
