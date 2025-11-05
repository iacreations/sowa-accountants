import random
from . models import Expense

def generate_unique_ref_no() -> str:
    """Return an 8-digit, zero-padded, numeric reference that isn't used yet."""
    for _ in range(10):  # a few attempts in case of a rare collision
        ref = f"{random.randrange(10**8):08d}"
        if not Expense.objects.filter(ref_no=ref).exists():
            return ref
    # If we somehow failed 10 times, raise; caller can handle or retry
    raise RuntimeError("Could not generate a unique reference number.")

def generate_unique_bill_no() -> str:
    """Return an 8-digit, zero-padded, numeric reference that isn't used yet."""
    for _ in range(10):  # a few attempts in case of a rare collision
        ref = f"{random.randrange(10**8):08d}"
        if not Expense.objects.filter(ref_no=ref).exists():
            return ref
    # If we somehow failed 10 times, raise; caller can handle or retry
    raise RuntimeError("Could not generate a unique reference number.")
