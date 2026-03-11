from decimal import Decimal

from django.db import models
from django.utils import timezone
from django.conf import settings
from django.core.exceptions import ValidationError

from tenancy.base import TenantModel

from sowaf.models import Newcustomer, Newsupplier
from accounts.models import Account
from inventory.models import Product, Pclass, InventoryLocation


DEC = dict(max_digits=12, decimal_places=2)


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def _same_company(parent_company_id, obj, field_label):
    """
    Ensures related object belongs to the same company as parent transaction.
    Only checks if the related object has company_id.
    """
    if obj is None:
        return

    obj_company_id = getattr(obj, "company_id", None)
    if obj_company_id is not None and parent_company_id != obj_company_id:
        raise ValidationError({field_label: f"{field_label} must belong to the same company."})


# -------------------------------------------------------------------
# EXPENSE
# -------------------------------------------------------------------

class Expense(TenantModel):
    PAYMENT_METHODS = [
        ("cash", "Cash"),
        ("bank_transfer", "Bank Transfer"),
        ("mobile_money", "Mobile Money"),
        ("cheque", "Cheque"),
        ("card", "Card"),
    ]

    payee_name = models.CharField(max_length=255, blank=True)
    payee_supplier = models.ForeignKey(Newsupplier, null=True, blank=True, on_delete=models.CASCADE)

    payment_account = models.ForeignKey(Account, on_delete=models.CASCADE)
    payment_date = models.DateField(default=timezone.localdate)
    payment_method = models.CharField(max_length=40, choices=PAYMENT_METHODS, default="cash")
    ref_no = models.CharField(max_length=50, blank=True)

    location = models.ForeignKey(
        InventoryLocation,
        null=True, blank=True,
        on_delete=models.CASCADE,
        related_name="expenses",
    )

    memo = models.TextField(blank=True)
    attachments = models.FileField(upload_to="expense_attachments/", blank=True, null=True)

    total_amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    created_at = models.DateTimeField(auto_now_add=True)

    journal_entry = models.OneToOneField(
        "accounts.JournalEntry",
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="expense_source",
    )

    is_posted = models.BooleanField(default=False)
    posted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-payment_date", "-id"]
        indexes = [
            models.Index(fields=["company", "payment_date"]),
            models.Index(fields=["company", "is_posted"]),
        ]

    def clean(self):
        super().clean()
        _same_company(self.company_id, self.payee_supplier, "payee_supplier")
        _same_company(self.company_id, self.payment_account, "payment_account")
        _same_company(self.company_id, self.location, "location")
        _same_company(self.company_id, self.journal_entry, "journal_entry")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        who = self.payee_supplier.company_name if self.payee_supplier else self.payee_name or "Payee"
        return f"{self.company.name} | Expense {self.id} - {who} ({self.payment_date})"

    @property
    def payee_display(self):
        return self.payee_supplier.company_name if self.payee_supplier else (self.payee_name or "—")

    @property
    def type_display(self):
        return "Expense"

    @property
    def number_display(self):
        return self.ref_no or f"{self.pk:06d}"

    @property
    def location_display(self):
        return self.location.name if self.location_id else "—"

    @property
    def category_display(self):
        total_lines = getattr(self, "_total_lines", None)
        if total_lines is None:
            total_lines = self.cat_lines.count() + self.item_lines.count()

        if total_lines > 1:
            return "--Split--"

        cat = next(iter(self.cat_lines.all()), None)
        if cat:
            return getattr(cat.category, "account_name", "—")

        item = next(iter(self.item_lines.all()), None)
        if item:
            return getattr(item.product, "name", "—")

        return "—"

    @property
    def total_before_tax(self):
        return self.total_amount

    @property
    def sales_tax_amount(self):
        return Decimal("0.00")

    @property
    def total_display(self):
        return self.total_amount

    @property
    def approval_status(self):
        return "—"


class ExpenseCategoryLine(models.Model):
    """Category details rows (GL expense accounts)."""
    BILL_STATUS = [("unbilled", "Unbilled"), ("billed", "Billed")]

    expense = models.ForeignKey(Expense, on_delete=models.CASCADE, related_name="cat_lines")
    category = models.ForeignKey(Account, on_delete=models.CASCADE)
    description = models.CharField(max_length=255, blank=True)
    amount = models.DecimalField(**DEC, default=Decimal("0.00"))

    is_billable = models.BooleanField(default=False)
    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE)
    bill_status = models.CharField(max_length=10, choices=BILL_STATUS, default="unbilled")

    def clean(self):
        super().clean()
        if not self.expense_id:
            return

        company_id = self.expense.company_id
        _same_company(company_id, self.category, "category")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.class_field, "class_field")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"Category line {self.category} {self.amount}"


class ExpenseItemLine(models.Model):
    """Item details rows (products/services)."""
    BILL_STATUS = [("unbilled", "Unbilled"), ("billed", "Billed")]

    expense = models.ForeignKey(Expense, on_delete=models.CASCADE, related_name="item_lines")
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    description = models.CharField(max_length=255, blank=True)

    qty = models.DecimalField(**DEC, default=Decimal("0.00"))
    rate = models.DecimalField(**DEC, default=Decimal("0.00"))
    amount = models.DecimalField(**DEC, default=Decimal("0.00"))

    is_billable = models.BooleanField(default=False)
    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE)
    bill_status = models.CharField(max_length=10, choices=BILL_STATUS, default="unbilled")

    def clean(self):
        super().clean()
        if not self.expense_id:
            return

        company_id = self.expense.company_id
        _same_company(company_id, self.product, "product")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.class_field, "class_field")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"Item line {self.product} x {self.qty} @ {self.rate}"


class ColumnPreference(TenantModel):
    """
    Tenant-safe column preferences for expenses views.
    """
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="expense_column_preferences",
    )
    table_name = models.CharField(max_length=100)
    preferences = models.JSONField(default=dict)

    class Meta:
        unique_together = ("company", "user", "table_name")
        indexes = [
            models.Index(fields=["company", "user", "table_name"]),
        ]

    def clean(self):
        super().clean()
        # user usually does not carry company directly in your setup,
        # membership is handled in tenancy, so no cross-company validation here.
        pass

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.company.name} - {self.user} - {self.table_name}"


# -------------------------------------------------------------------
# BILLS
# -------------------------------------------------------------------

class Bill(TenantModel):
    supplier = models.ForeignKey(Newsupplier, null=True, blank=True, on_delete=models.CASCADE)
    supplier_name = models.CharField(max_length=255, blank=True, null=True)

    mailing_address = models.CharField(max_length=255, blank=True, null=True)
    terms = models.CharField(max_length=100, blank=True, null=True)

    bill_date = models.DateField(default=timezone.localdate)
    due_date = models.DateField(blank=True, null=True)
    bill_no = models.CharField(max_length=32, unique=True)

    location = models.ForeignKey(
        InventoryLocation,
        null=True, blank=True,
        on_delete=models.PROTECT,
        related_name="bills",
    )

    memo = models.TextField(blank=True, null=True)
    attachments = models.FileField(upload_to="bills/", blank=True, null=True)

    total_amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    is_posted = models.BooleanField(default=False)
    posted_at = models.DateTimeField(null=True, blank=True)

    journal_entry = models.OneToOneField(
        "accounts.JournalEntry",
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="bill_source",
    )

    class Meta:
        ordering = ["-bill_date", "-id"]
        indexes = [
            models.Index(fields=["company", "bill_date"]),
            models.Index(fields=["company", "supplier"]),
        ]

    def clean(self):
        super().clean()
        _same_company(self.company_id, self.supplier, "supplier")
        _same_company(self.company_id, self.location, "location")
        _same_company(self.company_id, self.journal_entry, "journal_entry")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        who = self.supplier.company_name if self.supplier else (self.supplier_name or "")
        return f"{self.company.name} | Bill {self.bill_no} – {who}".strip()


class BillCategoryLine(models.Model):
    bill = models.ForeignKey(Bill, related_name="category_lines", on_delete=models.CASCADE)
    category = models.ForeignKey(Account, on_delete=models.CASCADE)
    description = models.CharField(max_length=255, blank=True, null=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    is_billable = models.BooleanField(default=False)
    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE)

    def clean(self):
        super().clean()
        if not self.bill_id:
            return

        company_id = self.bill.company_id
        _same_company(company_id, self.category, "category")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.class_field, "class_field")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.category} - {self.amount}"


class BillItemLine(models.Model):
    bill = models.ForeignKey(Bill, related_name="item_lines", on_delete=models.CASCADE)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    description = models.CharField(max_length=255, blank=True, null=True)

    qty = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    rate = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    is_billable = models.BooleanField(default=False)
    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE)

    def clean(self):
        super().clean()
        if not self.bill_id:
            return

        company_id = self.bill.company_id
        _same_company(company_id, self.product, "product")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.class_field, "class_field")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.product} x{self.qty} @ {self.rate} = {self.amount}"


# -------------------------------------------------------------------
# CHEQUE
# -------------------------------------------------------------------

class Cheque(TenantModel):
    payee_name = models.CharField(max_length=255, blank=True)
    payee_supplier = models.ForeignKey(Newsupplier, null=True, blank=True, on_delete=models.CASCADE)

    bank_account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name="cheque_payments")
    mailing_address = models.CharField(max_length=255, blank=True)
    payment_date = models.DateField(default=timezone.localdate)
    cheque_no = models.CharField(max_length=20, unique=True)

    location = models.ForeignKey(
        InventoryLocation,
        null=True, blank=True,
        on_delete=models.PROTECT,
        related_name="cheques",
    )

    memo = models.TextField(blank=True)
    attachments = models.FileField(upload_to="attachments/", null=True, blank=True)

    total_amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-payment_date", "-id"]
        indexes = [
            models.Index(fields=["company", "payment_date"]),
        ]

    def clean(self):
        super().clean()
        _same_company(self.company_id, self.payee_supplier, "payee_supplier")
        _same_company(self.company_id, self.bank_account, "bank_account")
        _same_company(self.company_id, self.location, "location")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.company.name} | Cheque #{self.cheque_no}"


class ChequeCategoryLine(models.Model):
    cheque = models.ForeignKey(Cheque, related_name="category_lines", on_delete=models.CASCADE)
    category = models.ForeignKey(Account, on_delete=models.CASCADE)
    description = models.CharField(max_length=255, blank=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    is_billable = models.BooleanField(default=False)
    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE)

    def clean(self):
        super().clean()
        if not self.cheque_id:
            return

        company_id = self.cheque.company_id
        _same_company(company_id, self.category, "category")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.class_field, "class_field")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.category} - {self.amount}"


class ChequeItemLine(models.Model):
    cheque = models.ForeignKey(Cheque, related_name="item_lines", on_delete=models.CASCADE)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    description = models.CharField(max_length=255, blank=True)

    qty = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    rate = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    is_billable = models.BooleanField(default=False)
    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE)

    def clean(self):
        super().clean()
        if not self.cheque_id:
            return

        company_id = self.cheque.company_id
        _same_company(company_id, self.product, "product")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.class_field, "class_field")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.product} x{self.qty} @ {self.rate} = {self.amount}"


class ChequeBillLine(models.Model):
    cheque = models.ForeignKey(Cheque, on_delete=models.CASCADE, related_name="bill_lines")
    bill = models.ForeignKey(Bill, on_delete=models.CASCADE, related_name="cheque_bill_lines")
    amount_applied = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        unique_together = ("cheque", "bill")

    def clean(self):
        super().clean()
        if self.cheque_id and self.bill_id:
            if self.cheque.company_id != self.bill.company_id:
                raise ValidationError("Cheque and Bill must belong to the same company.")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"Cheque {self.cheque_id} -> Bill {self.bill_id}: {self.amount_applied}"


class ChequeOpenBalanceLine(models.Model):
    cheque = models.OneToOneField(Cheque, on_delete=models.CASCADE, related_name="open_balance_line")
    amount_applied = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    def clean(self):
        super().clean()
        if self.amount_applied < 0:
            raise ValidationError({"amount_applied": "Amount applied cannot be negative."})

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"Cheque {self.cheque_id} Open Balance: {self.amount_applied}"


# -------------------------------------------------------------------
# PURCHASE ORDER
# -------------------------------------------------------------------

class PurchaseOrder(TenantModel):
    vendor = models.ForeignKey(Newsupplier, null=True, blank=True, on_delete=models.CASCADE)
    vendor_name = models.CharField(max_length=255, blank=True)

    mailing_address = models.CharField(max_length=255, blank=True)
    po_date = models.DateField(default=timezone.localdate)
    deliver_by = models.DateField(null=True, blank=True)
    ship_to = models.CharField(max_length=255, blank=True)

    location = models.ForeignKey(
        InventoryLocation,
        null=True, blank=True,
        on_delete=models.PROTECT,
        related_name="purchase_orders",
    )

    po_number = models.CharField(max_length=20, unique=True)
    memo = models.TextField(blank=True)
    attachments = models.FileField(upload_to="purchase_orders/", null=True, blank=True)

    total_amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    STATUS_CHOICES = (
        ("draft", "Draft"),
        ("sent", "Sent"),
        ("closed", "Closed"),
        ("cancelled", "Cancelled"),
    )
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="draft")

    class Meta:
        ordering = ["-po_date", "-id"]
        indexes = [
            models.Index(fields=["company", "po_date"]),
        ]

    def clean(self):
        super().clean()
        _same_company(self.company_id, self.vendor, "vendor")
        _same_company(self.company_id, self.location, "location")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.company.name} | PO {self.po_number or self.id}"


class PurchaseOrderLine(models.Model):
    purchase_order = models.ForeignKey(PurchaseOrder, related_name="lines", on_delete=models.CASCADE)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    description = models.CharField(max_length=255, blank=True)

    qty = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    rate = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE)

    def clean(self):
        super().clean()
        if not self.purchase_order_id:
            return

        company_id = self.purchase_order.company_id
        _same_company(company_id, self.product, "product")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.class_field, "class_field")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.product} x{self.qty} @ {self.rate}"


# -------------------------------------------------------------------
# SUPPLIER CREDIT
# -------------------------------------------------------------------

class SupplierCredit(TenantModel):
    supplier = models.ForeignKey(
        Newsupplier,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="supplier_credits",
    )
    supplier_name = models.CharField(max_length=255, blank=True, null=True)

    mailing_address = models.CharField(max_length=255, blank=True, null=True)
    credit_date = models.DateField(default=timezone.localdate)
    ref_no = models.CharField(max_length=20, unique=True)

    location = models.ForeignKey(
        InventoryLocation,
        null=True, blank=True,
        on_delete=models.PROTECT,
        related_name="supplier_credits",
    )

    memo = models.TextField(blank=True, null=True)
    attachments = models.FileField(upload_to="attachments/", blank=True, null=True)

    total_amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-credit_date", "-id"]
        indexes = [
            models.Index(fields=["company", "credit_date"]),
        ]

    def clean(self):
        super().clean()
        _same_company(self.company_id, self.supplier, "supplier")
        _same_company(self.company_id, self.location, "location")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.company.name} | Supplier Credit {self.ref_no or self.id}"


class SupplierCreditLine(models.Model):
    supplier_credit = models.ForeignKey(SupplierCredit, related_name="lines", on_delete=models.CASCADE)

    line_date = models.DateField(blank=True, null=True)
    category = models.ForeignKey(Account, on_delete=models.CASCADE)
    description = models.CharField(max_length=255, blank=True, null=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    is_billable = models.BooleanField(default=False)
    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE)

    def clean(self):
        super().clean()
        if not self.supplier_credit_id:
            return

        company_id = self.supplier_credit.company_id
        _same_company(company_id, self.category, "category")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.class_field, "class_field")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"Supplier Credit Line {self.id} ({self.amount})"


# -------------------------------------------------------------------
# PAY DOWN CREDIT
# -------------------------------------------------------------------

class PayDownCredit(TenantModel):
    credit_card = models.ForeignKey(Account, on_delete=models.CASCADE, related_name="credit_card_paydowns")
    bank_account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name="paydown_bank_accounts")

    payee_supplier = models.ForeignKey(Newsupplier, null=True, blank=True, on_delete=models.CASCADE)
    payee_name = models.CharField(max_length=255, blank=True)

    payment_date = models.DateField(default=timezone.localdate)
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    ref_no = models.CharField(max_length=20, blank=True)

    location = models.ForeignKey(
        InventoryLocation,
        null=True, blank=True,
        on_delete=models.PROTECT,
        related_name="paydown_credits",
    )

    memo = models.TextField(blank=True)
    attachments = models.FileField(upload_to="attachments/", null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-payment_date", "-id"]
        indexes = [
            models.Index(fields=["company", "payment_date"]),
        ]

    def clean(self):
        super().clean()
        _same_company(self.company_id, self.credit_card, "credit_card")
        _same_company(self.company_id, self.bank_account, "bank_account")
        _same_company(self.company_id, self.payee_supplier, "payee_supplier")
        _same_company(self.company_id, self.location, "location")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.company.name} | Pay down {self.credit_card.account_name} ({self.amount})"


# -------------------------------------------------------------------
# CREDIT CARD CREDIT
# -------------------------------------------------------------------

class CreditCardCredit(TenantModel):
    credit_card = models.ForeignKey(
        Account,
        on_delete=models.CASCADE,
        related_name="credit_card_credits",
        help_text="The credit card account this credit applies to.",
    )

    payee_supplier = models.ForeignKey(
        Newsupplier,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="credit_card_credits",
    )
    payee_name = models.CharField(max_length=255, blank=True)

    credit_date = models.DateField(default=timezone.localdate)
    ref_no = models.CharField(max_length=30, blank=True)

    location = models.ForeignKey(
        InventoryLocation,
        null=True, blank=True,
        on_delete=models.PROTECT,
        related_name="credit_card_credits",
    )

    tags = models.CharField(max_length=255, blank=True)
    memo = models.TextField(blank=True)

    attachments = models.FileField(upload_to="attachments/", null=True, blank=True)

    total_amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-credit_date", "-id"]
        indexes = [
            models.Index(fields=["company", "credit_date"]),
        ]

    def clean(self):
        super().clean()
        _same_company(self.company_id, self.credit_card, "credit_card")
        _same_company(self.company_id, self.payee_supplier, "payee_supplier")
        _same_company(self.company_id, self.location, "location")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.company.name} | CC Credit {self.ref_no or self.id} ({self.total_amount})"


class CreditCardCreditCategoryLine(models.Model):
    credit = models.ForeignKey(CreditCardCredit, on_delete=models.CASCADE, related_name="category_lines")
    category = models.ForeignKey(Account, on_delete=models.PROTECT, related_name="cc_credit_category_lines")

    description = models.CharField(max_length=255, blank=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    billable = models.BooleanField(default=False)

    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE, related_name="cc_credit_category_lines")
    pclass = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE, related_name="cc_credit_category_lines")

    def clean(self):
        super().clean()
        if not self.credit_id:
            return

        company_id = self.credit.company_id
        _same_company(company_id, self.category, "category")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.pclass, "pclass")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"CC Credit Cat Line {self.id} ({self.amount})"


class CreditCardCreditItemLine(models.Model):
    credit = models.ForeignKey(CreditCardCredit, on_delete=models.CASCADE, related_name="item_lines")

    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="cc_credit_item_lines")
    description = models.CharField(max_length=255, blank=True)

    quantity = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    rate = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    billable = models.BooleanField(default=False)

    customer = models.ForeignKey(Newcustomer, null=True, blank=True, on_delete=models.CASCADE, related_name="cc_credit_item_lines")
    pclass = models.ForeignKey(Pclass, null=True, blank=True, on_delete=models.CASCADE, related_name="cc_credit_item_lines")

    def clean(self):
        super().clean()
        if not self.credit_id:
            return

        company_id = self.credit.company_id
        _same_company(company_id, self.product, "product")
        _same_company(company_id, self.customer, "customer")
        _same_company(company_id, self.pclass, "pclass")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"CC Credit Item Line {self.id} ({self.amount})"


# -------------------------------------------------------------------
# SUPPLIER REFUND (TENANT SAFE)
# -------------------------------------------------------------------

class SupplierRefund(TenantModel):
    supplier = models.ForeignKey(Newsupplier, on_delete=models.CASCADE, related_name="supplier_refunds")
    refund_date = models.DateField(default=timezone.localdate)

    received_to = models.ForeignKey(
        Account,
        on_delete=models.PROTECT,
        related_name="supplier_refund_received_to",
    )

    reference_no = models.CharField(max_length=50, blank=True, null=True)
    memo = models.TextField(blank=True, null=True)

    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        ordering = ["-refund_date", "-id"]
        indexes = [
            models.Index(fields=["company", "refund_date"]),
            models.Index(fields=["company", "supplier"]),
        ]

    def clean(self):
        super().clean()
        _same_company(self.company_id, self.supplier, "supplier")
        _same_company(self.company_id, self.received_to, "received_to")

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.company.name} | Supplier Refund {self.id} - {self.supplier} ({self.amount})"