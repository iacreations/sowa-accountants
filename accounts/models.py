from django.db import models
from django.utils import timezone
from django.conf import settings




class Account(models.Model):
    ACCOUNT_TYPES = [
        ("AR", "Accounts Receivable (A/R)"),
        ("CURRENT_ASSET", "Current Assets"),
        ("CASH_EQUIV", "Cash and Cash Equivalents"),
        ("FIXED_ASSET", "Fixed Assets"),
        ("NON-CURRENT-ASSET", "Non-Current Assets"),
        ("AP", "Accounts Payable (A/P)"),
        ("CREDIT_CARD", "Credit Card"),
        ("CURRENT_LIABILITY", "Current Liabilities"),
        ("NON-CURRENT-LIABILITY", "Non-Current Liabilities"),
        ("OWNER_EQUITY", "Owner's Equity"),
        ("INCOME", "Income"),
        ("OTHER_INCOME", "Other Income"),
        ("COST_OF_SALES", "Cost of Sales"),
        ("EXPENSE", "Expenses"),
        ("OTHER_EXPENSE", "Other Expenses"),
    ]

    # Main fields
    account_name = models.CharField(max_length=255,blank=True, null=True)
    account_number = models.CharField(max_length=255, blank=True, null=True)
    account_type = models.CharField(max_length=255,blank=True, null=True)
    detail_type = models.CharField(max_length=255, blank=True, null=True)

    # Subaccount (self reference)
    is_subaccount = models.BooleanField(default=False)
    parent = models.ForeignKey(
        "self", on_delete=models.CASCADE,
        related_name="children",
        null=True, blank=True
    )

    # Balance info
    opening_balance = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    as_of = models.DateField(default=timezone.now)

    # Extra
    description = models.TextField(blank=True, null=True)
    created_at = models.DateField(auto_now_add=True, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    def __str__(self):
        return f"Account name: {self.account_name} | Account type: {self.account_type} | Detail type: {self.detail_type}"
    # making the customization table
class ColumnPreference(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE,related_name='account_column_preferences')
    table_name = models.CharField(max_length=100)  # e.g. "accounts"
    preferences = models.JSONField(default=dict)   # store {col_name: true/false}

    class Meta:
        unique_together = ('user', 'table_name')

    def __str__(self):
        return f"{self.user} - {self.table_name}"

# allowing posting of sales to COA

class JournalEntry(models.Model):
    date = models.DateField(default=timezone.now)
    description = models.TextField(blank=True, null=True)
    invoice = models.ForeignKey("sales.Newinvoice", blank=True, null=True, on_delete=models.CASCADE)
    expense  = models.ForeignKey("expenses.Expense", blank=True, null=True, on_delete=models.CASCADE)
    def __str__(self):
        return f"Journal Entry {self.id} - {self.date}"


class JournalLine(models.Model):
    entry = models.ForeignKey(JournalEntry, related_name="lines", on_delete=models.CASCADE)
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    debit = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    credit = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    def __str__(self):
        return f"{self.account} DR:{self.debit} CR:{self.credit}"