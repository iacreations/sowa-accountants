# expenses/signals.py
"""
Register inventory signals for expense documents (Cheque).
"""
from inventory.signals import connect_cheque_signals

connect_cheque_signals()
