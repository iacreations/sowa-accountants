from django.db import models
from django.utils import timezone
from decimal import Decimal
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder

class Account(models.Model):
    # Store CODES internally, show labels in UI
    ACCOUNT_TYPES = [
        ("NON_CURRENT_ASSET", "Non current assets"),
        ("CURRENT_ASSET", "Current Assets"),

        ("OWNER_EQUITY", "Owner's Equity"),

        ("NON_CURRENT_LIABILITY", "Non Current Liabilities"),
        ("CURRENT_LIABILITY", "Current Liabilities"),

        ("OPERATING_INCOME", "Operating Income"),
        ("INVESTING_INCOME", "Investing Income"),

        ("OPERATING_EXPENSE", "Operating Expense"),
        ("INVESTING_EXPENSE", "Investing Expense"),
        ("FINANCING_EXPENSE", "Financing Expense"),
        ("INCOME_TAX_EXPENSE", "Income taxes"),
    ]

    # LEVEL 1: must always be one of these 5
    ACCOUNT_LEVEL1_MAP = {
        # Assets
        "NON_CURRENT_ASSET": "Assets",
        "CURRENT_ASSET": "Assets",

        # Equity
        "OWNER_EQUITY": "Equity",

        # Liabilities
        "NON_CURRENT_LIABILITY": "Liabilities",
        "CURRENT_LIABILITY": "Liabilities",

        # Income (both operating & investing)
        "OPERATING_INCOME": "Income",
        "INVESTING_INCOME": "Income",

        # Expenses (all types, including tax)
        "OPERATING_EXPENSE": "Expenses",
        "INVESTING_EXPENSE": "Expenses",
        "FINANCING_EXPENSE": "Expenses",
        "INCOME_TAX_EXPENSE": "Expenses",
    }

    # LEVEL 2: sub-groups (optional, for future reporting)
    ACCOUNT_LEVEL2_MAP = {
        # Assets
        "NON_CURRENT_ASSET": "Non current assets",
        "CURRENT_ASSET": "Current Assets",

        # Equity
        "OWNER_EQUITY": "Owner's Equity",

        # Liabilities
        "NON_CURRENT_LIABILITY": "Non Current Liabilities",
        "CURRENT_LIABILITY": "Current Liabilities",

        # Income
        "OPERATING_INCOME": "Operating Income",
        "INVESTING_INCOME": "Investing Income",

        # Expenses
        "OPERATING_EXPENSE": "Operating Expense",
        "INVESTING_EXPENSE": "Investing Expense",
        "FINANCING_EXPENSE": "Financing Expense",
        "INCOME_TAX_EXPENSE": "Income taxes",
    }

    # Main fields
    account_name = models.CharField(max_length=255, blank=True, null=True)
    account_number = models.CharField(max_length=255, blank=True, null=True)

    # store account type code
    account_type = models.CharField(
        max_length=50,
        choices=ACCOUNT_TYPES,
        blank=True,
        null=True,
    )

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

    tax_category = models.CharField(
        max_length=50,
        blank=True,
        null=True,
        choices=[
            ("Capital deductions", "Capital deductions"),
            ("Allowable deduction", "Allowable deduction"),
            ("Non Allowable deduction", "Non Allowable deduction"),
        ],
    )

    def __str__(self):
        return (
            f"Account name: {self.account_name} | "
            f"Account type: {self.account_type} | "
            f"Detail type: {self.detail_type}"
        )

    # ----- convenience properties -----

    @property
    def level1_group(self) -> str | None:
        """
        One of: Assets, Liabilities, Equity, Income, Expenses
        """
        if not self.account_type:
            return None
        return self.ACCOUNT_LEVEL1_MAP.get(self.account_type)

    @property
    def level2_group(self) -> str | None:
        """
        Sub-group like 'Operating Income', 'Operating Expense', etc.
        """
        if not self.account_type:
            return None
        return self.ACCOUNT_LEVEL2_MAP.get(self.account_type)


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

# Journal entries 

class JournalEntry(models.Model):
    date = models.DateField()
    description = models.CharField(max_length=255, blank=True, null=True)

    # optional linkage to source documents (invoice, bill, etc.)
    source_type = models.CharField(max_length=50, blank=True, null=True)
    source_id = models.IntegerField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.date} - {self.description or 'Journal Entry'}"


class JournalLine(models.Model):
    entry = models.ForeignKey(JournalEntry, on_delete=models.CASCADE, related_name="lines")
    account = models.ForeignKey("accounts.Account", on_delete=models.PROTECT)

    debit = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    credit = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    # ✅ Sub-ledger links (THIS is what your client means)
    supplier = models.ForeignKey(
        "sowaf.Newsupplier",  # change app label if different
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="ap_lines",
    )
    customer = models.ForeignKey(
        "sowaf.Newcustomer",  # change app label if different
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="ar_lines",
    )

    def __str__(self):
        return f"{self.account} DR {self.debit} CR {self.credit}"

# audit trail

# class AuditTrail(models.Model):
#     ACTION_CHOICES = (
#         ("CREATE", "Create"),
#         ("UPDATE", "Update"),
#         ("DELETE", "Delete"),
#         ("LOGIN", "Login"),
#         ("LOGOUT", "Logout"),
#     )

#     user = models.ForeignKey(
#         settings.AUTH_USER_MODEL, 
#         null=True, blank=True, 
#         on_delete=models.SET_NULL
#     )

#     action = models.CharField(max_length=10, choices=ACTION_CHOICES)
#     model_name = models.CharField(max_length=100)
#     object_id = models.PositiveIntegerField(null=True, blank=True)
#     description = models.TextField()

#     old_data = models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)
#     new_data = models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

#     ip_address = models.GenericIPAddressField(null=True, blank=True)
#     timestamp = models.DateTimeField(default=timezone.now)

#     class Meta:
#         ordering = ["-timestamp"]

#     def __str__(self):
#         return f"{self.action} {self.model_name} ({self.object_id})"
