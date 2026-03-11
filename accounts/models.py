from decimal import Decimal
from django.db import models
from django.utils import timezone
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.core.exceptions import ValidationError

from tenancy.base import TenantModel


class Account(TenantModel):
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
    opening_balance = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
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

    class Meta:
        indexes = [
            models.Index(fields=["company", "account_type"]),
            models.Index(fields=["company", "is_active"]),
            models.Index(fields=["company", "account_number"]),
        ]

        constraints = [
            models.UniqueConstraint(
                fields=["company", "account_number"],
                name="uniq_account_number_per_company",
                condition=models.Q(account_number__isnull=False),
            )
        ]

    def __str__(self):
        return f"{self.account_name or 'Account'} ({self.company.name})"

    def clean(self):
        errors = {}

        if self.parent_id:
            if self.parent.company_id != self.company_id:
                errors["parent"] = "Parent account must belong to the same company."

            if self.parent_id == self.id:
                errors["parent"] = "An account cannot be its own parent."

        # keep is_subaccount consistent with parent
        if self.parent_id and not self.is_subaccount:
            self.is_subaccount = True

        if not self.parent_id and self.is_subaccount:
            self.is_subaccount = False

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    # ----- convenience properties -----

    @property
    def level1_group(self):
        """
        One of: Assets, Liabilities, Equity, Income, Expenses
        """
        if not self.account_type:
            return None
        return self.ACCOUNT_LEVEL1_MAP.get(self.account_type)

    @property
    def level2_group(self):
        """
        Sub-group like 'Operating Income', 'Operating Expense', etc.
        """
        if not self.account_type:
            return None
        return self.ACCOUNT_LEVEL2_MAP.get(self.account_type)


class ColumnPreference(TenantModel):
    """
    IMPORTANT: Must be tenant-scoped so the same user can have different
    column settings per company.
    """
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="account_column_preferences",
    )
    table_name = models.CharField(max_length=100)  # e.g. "accounts"
    preferences = models.JSONField(default=dict)   # store {col_name: true/false}

    class Meta:
        unique_together = ("company", "user", "table_name")
        indexes = [
            models.Index(fields=["company", "user", "table_name"]),
        ]

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        company_name = self.company.name if self.company else "No Company"
        return f"{company_name} - {self.user} - {self.table_name}"


# ----------------- Journal Entries -----------------

class JournalEntry(TenantModel):
    date = models.DateField()
    description = models.CharField(max_length=255, blank=True, null=True)

    # optional linkage to source documents (invoice, bill, etc.)
    source_type = models.CharField(max_length=50, blank=True, null=True)
    source_id = models.IntegerField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["company", "date"]),
            models.Index(fields=["company", "source_type", "source_id"]),
        ]

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.company.name} | {self.date} - {self.description or 'Journal Entry'}"


class JournalLine(models.Model):
    """
    Journal lines belong to a JournalEntry, which already belongs to a company.
    So we do NOT store company here (avoids inconsistency).
    """
    entry = models.ForeignKey(JournalEntry, on_delete=models.CASCADE, related_name="lines")
    account = models.ForeignKey("accounts.Account", on_delete=models.PROTECT)

    debit = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    credit = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    # Sub-ledger links
    supplier = models.ForeignKey(
        "sowaf.Newsupplier",
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="ap_lines",
    )
    customer = models.ForeignKey(
        "sowaf.Newcustomer",
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="ar_lines",
    )

    class Meta:
        indexes = [
            models.Index(fields=["entry", "account"]),
        ]

    def __str__(self):
        return f"{self.account} DR {self.debit} CR {self.credit}"

    def clean(self):
        """
        Safety: ensure related objects belong to the same company as entry.
        """
        errors = {}

        if self.entry_id and self.account_id:
            if self.entry.company_id != self.account.company_id:
                errors["account"] = "JournalLine account and entry must belong to the same company."

        if self.entry_id and self.supplier_id:
            supplier_company_id = getattr(self.supplier, "company_id", None)
            if supplier_company_id is not None and supplier_company_id != self.entry.company_id:
                errors["supplier"] = "Supplier must belong to the same company as the journal entry."

        if self.entry_id and self.customer_id:
            customer_company_id = getattr(self.customer, "company_id", None)
            if customer_company_id is not None and customer_company_id != self.entry.company_id:
                errors["customer"] = "Customer must belong to the same company as the journal entry."

        if self.debit < 0:
            errors["debit"] = "Debit cannot be negative."

        if self.credit < 0:
            errors["credit"] = "Credit cannot be negative."

        if self.debit > 0 and self.credit > 0:
            errors["credit"] = "A journal line cannot have both debit and credit values."

        if self.debit == 0 and self.credit == 0:
            errors["debit"] = "A journal line must have either a debit or a credit amount."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        # enforce company consistency
        self.full_clean()
        super().save(*args, **kwargs)


# ----------------- Audit Trail -----------------

class AuditTrail(models.Model):
    """
    You can optionally also add company here, but it's not required.
    If you want per-company audit logs, add:
        company = models.ForeignKey("tenancy.Company", ...)
    """
    ACTION_CHOICES = (
        ("CREATE", "Create"),
        ("UPDATE", "Update"),
        ("DELETE", "Delete"),
        ("LOGIN", "Login"),
        ("LOGOUT", "Logout"),
    )

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL
    )

    action = models.CharField(max_length=10, choices=ACTION_CHOICES)
    model_name = models.CharField(max_length=100)
    object_id = models.PositiveIntegerField(null=True, blank=True)
    description = models.TextField()

    old_data = models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)
    new_data = models.JSONField(null=True, blank=True, encoder=DjangoJSONEncoder)

    ip_address = models.GenericIPAddressField(null=True, blank=True)
    timestamp = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-timestamp"]
        indexes = [
            models.Index(fields=["timestamp"]),
            models.Index(fields=["user", "timestamp"]),
        ]

    def __str__(self):
        return f"{self.action} {self.model_name} ({self.object_id})"