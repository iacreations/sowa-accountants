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

    # Level 1: top categories used in Balance Sheet & P&L
    # (This is NOT stored in DB, it's derived from account_type)
    ACCOUNT_LEVEL1_MAP = {
        # ASSETS
        "Accounts Receivable (A/R)": "Assets",
        "Current Assets": "Assets",
        "Cash and Cash Equivalents": "Assets",
        "Fixed Assets": "Assets",
        "Non-Current Assets": "Assets",

        # EQUITY & LIABILITIES
        "Accounts Payable (A/P)": "Equity and Liabilities",
        "Credit Card": "Equity and Liabilities",
        "Current Liabilities": "Equity and Liabilities",
        "Non-current Liabilities": "Equity and Liabilities",
        "Owner's Equity": "Equity and Liabilities",

        # P&L – we keep Income/Expenses as their own level 1 groups
        "Income": "Income",
        "Other Income": "Income",
        "Cost of Sales": "Expenses",
        "Expenses": "Expenses",
        "Other Expenses": "Expenses",
    }

    # Level 2: subheadings under each Level 1 group (Excel-style)
    ACCOUNT_LEVEL2_MAP = {
        # ASSETS: Level 2 subgroups
        "Accounts Receivable (A/R)": "Current Assets",
        "Current Assets": "Current Assets",
        "Cash and Cash Equivalents": "Current Assets",
        "Fixed Assets": "Non-current Assets",
        "Non-Current Assets": "Non-current Assets",

        # EQUITY & LIABILITIES: Level 2 subgroups
        "Accounts Payable (A/P)": "Current Liabilities",
        "Credit Card": "Current Liabilities",
        "Current Liabilities": "Current Liabilities",
        "Non-current Liabilities": "Non-current Liabilities",
        "Owner's Equity": "Owner's Equity",

        # P&L: Level 2 structure (we can refine later: operating/investing/financing)
        "Income": "Operating Income",
        "Other Income": "Other Income",
        "Cost of Sales": "Cost of Sales",
        "Expenses": "Operating Expenses",
        "Other Expenses": "Other Expenses",
    }

    # Main fields
    account_name = models.CharField(max_length=255, blank=True, null=True)
    account_number = models.CharField(max_length=255, blank=True, null=True)
    account_type = models.CharField(max_length=255, blank=True, null=True)
    detail_type = models.CharField(max_length=255, blank=True, null=True)

    # Subaccount (self reference)
    is_subaccount = models.BooleanField(default=False)
    parent = models.ForeignKey(
        "self",
        on_delete=models.CASCADE,
        related_name="children",
        null=True,
        blank=True,
    )

    # Balance info
    opening_balance = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    as_of = models.DateField(default=timezone.now)

    # Extra
    description = models.TextField(blank=True, null=True)
    created_at = models.DateField(auto_now_add=True, null=True, blank=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return (
            f"Account name: {self.account_name} | "
            f"Account type: {self.account_type} | "
            f"Detail type: {self.detail_type}"
        )

    # ----- NEW convenience properties for backend mapping -----

    @property
    def level1_group(self) -> str | None:
        """
        Derived Level 1 group for this account, based on account_type.
        Examples: 'Assets', 'Equity and Liabilities', 'Income', 'Expenses'.
        """
        if not self.account_type:
            return None
        return self.ACCOUNT_LEVEL1_MAP.get(self.account_type)

    @property
    def level2_group(self) -> str | None:
        """
        Derived Level 2 group for this account, based on account_type.
        Examples: 'Current Assets', 'Non-current Assets',
                  'Owner's Equity', 'Current Liabilities', etc.
        """
        if not self.account_type:
            return None
        return self.ACCOUNT_LEVEL2_MAP.get(self.account_type)


# making the customization table
class ColumnPreference(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="account_column_preferences",
    )
    table_name = models.CharField(max_length=100)  # e.g. "accounts"
    preferences = models.JSONField(default=dict)  # store {col_name: true/false}

    class Meta:
        unique_together = ("user", "table_name")

    def __str__(self):
        return f"{self.user} - {self.table_name}"


# allowing posting of sales to COA
class JournalEntry(models.Model):
    date = models.DateField(default=timezone.now)
    description = models.TextField(blank=True, null=True)
    invoice = models.ForeignKey(
        "sales.Newinvoice", blank=True, null=True, on_delete=models.CASCADE
    )
    expense = models.ForeignKey(
        "expenses.Expense", blank=True, null=True, on_delete=models.CASCADE
    )

    def __str__(self):
        return f"Journal Entry {self.id} - {self.date}"


class JournalLine(models.Model):
    entry = models.ForeignKey(
        JournalEntry, related_name="lines", on_delete=models.CASCADE
    )
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    debit = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    credit = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    def __str__(self):
        return f"{self.account} DR:{self.debit} CR:{self.credit}"
