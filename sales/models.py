from decimal import Decimal
from datetime import timedelta

from django.db import models
from django.db.models import Sum
from django.utils import timezone
from django.conf import settings

from tenancy.base import TenantModel

from accounts.models import Account
from sowaf.models import Newcustomer
from inventory.models import Product, Pclass, InventoryLocation


# -------------------------------------------------------------------
# INVOICES
# -------------------------------------------------------------------

class Newinvoice(TenantModel):
    date_created = models.DateTimeField(null=True, blank=True)
    due_date = models.DateTimeField(null=True, blank=True)

    customer = models.ForeignKey(Newcustomer, on_delete=models.CASCADE)
    email = models.EmailField(max_length=255, null=True, blank=True)
    billing_address = models.CharField(max_length=255, null=True, blank=True)
    shipping_address = models.CharField(max_length=255, null=True, blank=True)
    terms = models.CharField(max_length=255, null=True, blank=True)
    sales_rep = models.CharField(max_length=255, null=True, blank=True)

    class_field = models.ForeignKey(Pclass, on_delete=models.CASCADE, blank=True, null=True)
    tags = models.CharField(max_length=255, null=True, blank=True)
    po_num = models.PositiveIntegerField(null=True, blank=True)
    memo = models.CharField(max_length=255, null=True, blank=True)
    customs_notes = models.CharField(max_length=255, null=True, blank=True)
    attachments = models.FileField(null=True, blank=True)

    # Location FK
    location = models.ForeignKey(
        InventoryLocation,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="invoices",
    )

    # totals (prefer Decimal in future, but keeping your floats to avoid breaking forms)
    subtotal = models.FloatField(default=0)
    total_discount = models.FloatField(default=0)
    shipping_fee = models.FloatField(default=0)
    total_vat = models.FloatField(default=0)
    total_due = models.FloatField(default=0)

    class Meta:
        ordering = ["-date_created", "-id"]
        indexes = [
            models.Index(fields=["company", "date_created"]),
            models.Index(fields=["company", "due_date"]),
            models.Index(fields=["company", "customer"]),
        ]

    def __str__(self):
        return f"{self.company.name} | {self.customer.customer_name} | #{self.id}"

    @property
    def amount_paid(self):
        return self.payments_applied.aggregate(total=Sum("amount_paid"))["total"] or Decimal("0.00")

    @property
    def balance(self):
        # total_due is float; amount_paid is Decimal -> convert
        total_due_dec = Decimal(str(self.total_due or 0))
        return total_due_dec - (self.amount_paid or Decimal("0.00"))


class InvoiceItem(models.Model):
    invoice = models.ForeignKey(Newinvoice, on_delete=models.CASCADE, related_name="items")

    # FK to product
    product = models.ForeignKey(Product, on_delete=models.CASCADE)

    # snapshots to preserve history
    name_snapshot = models.CharField(max_length=255, blank=True)
    description = models.TextField(blank=True, null=True)

    qty = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("1.00"))
    unit_price = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    # VAT stored as amount (not percent) based on your current usage
    vat = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)

    # final line amount after discount + vat
    amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    discount_num = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)
    discount_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["invoice", "product"]),
        ]

    def save(self, *args, **kwargs):
        # fill snapshots from product if not provided
        if self.product and not self.name_snapshot:
            self.name_snapshot = getattr(self.product, "name", "") or ""

        qty = self.qty or Decimal("0.00")
        unit_price = self.unit_price or Decimal("0.00")
        line_subtotal = qty * unit_price

        disc_amt = self.discount_amount or Decimal("0.00")
        vat_amt = self.vat or Decimal("0.00")

        # compute final amount
        self.amount = (line_subtotal - disc_amt) + vat_amt
        super().save(*args, **kwargs)

    def __str__(self):
        label = self.name_snapshot or (getattr(self.product, "name", None) or "Line")
        return f"{label} x {self.qty} (Invoice {self.invoice_id})"


# -------------------------------------------------------------------
# PAYMENTS
# -------------------------------------------------------------------

class Payment(TenantModel):
    PAYMENT_METHODS = [
        ("cash", "Cash"),
        ("bank_transfer", "Bank Transfer"),
        ("mobile_money", "Mobile Money"),
        ("cheque", "Cheque"),
    ]

    customer = models.ForeignKey(Newcustomer, on_delete=models.CASCADE, related_name="payments")
    payment_date = models.DateField()
    payment_method = models.CharField(max_length=50, choices=PAYMENT_METHODS)

    deposit_to = models.ForeignKey(
        Account,
        on_delete=models.CASCADE,
        related_name="payment_account",
    )

    reference_no = models.CharField(max_length=50, blank=True, null=True)
    tags = models.CharField(max_length=255, blank=True, null=True)
    memo = models.TextField(blank=True, null=True)

    amount_received = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    unapplied_amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        indexes = [
            models.Index(fields=["company", "payment_date"]),
            models.Index(fields=["company", "customer"]),
        ]

    def __str__(self):
        return f"{self.company.name} | Payment {self.id} - {self.customer.customer_name}"


class PaymentInvoice(models.Model):
    payment = models.ForeignKey(Payment, on_delete=models.CASCADE, related_name="applied_invoices")
    invoice = models.ForeignKey(Newinvoice, on_delete=models.CASCADE, related_name="payments_applied")
    amount_paid = models.DecimalField(max_digits=12, decimal_places=2)

    class Meta:
        indexes = [
            models.Index(fields=["payment", "invoice"]),
        ]

    def clean(self):
        # enforce tenant consistency
        if self.payment_id and self.invoice_id:
            if self.payment.company_id != self.invoice.company_id:
                raise ValueError("Payment and Invoice must belong to the same company.")

    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"Payment {self.payment.id} → Invoice {self.invoice.id} ({self.amount_paid})"


class PaymentOpenBalanceLine(models.Model):
    payment = models.OneToOneField(
        Payment,
        on_delete=models.CASCADE,
        related_name="open_balance_line",
    )
    amount_applied = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    def __str__(self):
        return f"Payment {self.payment_id} Open Balance Applied: {self.amount_applied}"


# -------------------------------------------------------------------
# SALES RECEIPTS
# -------------------------------------------------------------------

class SalesReceipt(TenantModel):
    PAYMENT_METHODS = [
        ("cash", "Cash"),
        ("bank_transfer", "Bank Transfer"),
        ("mobile_money", "Mobile Money"),
        ("cheque", "Cheque"),
    ]

    customer = models.ForeignKey(Newcustomer, on_delete=models.CASCADE, related_name="sales_receipts")
    receipt_date = models.DateField(default=timezone.now)
    payment_method = models.CharField(max_length=50, choices=PAYMENT_METHODS, default="cash")

    deposit_to = models.ForeignKey(Account, on_delete=models.CASCADE, related_name="sales_receipts")

    reference_no = models.CharField(max_length=50, blank=True, null=True)
    tags = models.CharField(max_length=255, blank=True, null=True)
    memo = models.TextField(blank=True, null=True)

    subtotal = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    total_discount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    total_vat = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    shipping_fee = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    total_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    amount_paid = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    balance = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-receipt_date", "-id"]
        indexes = [
            models.Index(fields=["company", "receipt_date"]),
            models.Index(fields=["company", "customer"]),
        ]

    def __str__(self):
        return f"{self.company.name} | Sales Receipt {self.id} - {self.customer.customer_name}"


class SalesReceiptLine(models.Model):
    receipt = models.ForeignKey(SalesReceipt, on_delete=models.CASCADE, related_name="lines")
    product = models.ForeignKey(Product, on_delete=models.CASCADE, null=True, blank=True)
    description = models.CharField(max_length=255, blank=True, null=True)

    qty = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    unit_price = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    discount_pct = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("0.00"))
    discount_amt = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    vat_amt = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        indexes = [
            models.Index(fields=["receipt", "product"]),
        ]

    def __str__(self):
        return f"SR#{self.receipt_id} - {self.description or (self.product and self.product.name) or 'Line'}"


# -------------------------------------------------------------------
# CUSTOMER STATEMENTS
# -------------------------------------------------------------------

class Statement(TenantModel):
    class StatementType(models.TextChoices):
        TRANSACTION = "transaction", "Transaction Statement"
        OPEN_ITEM = "open_item", "Open Item"
        BAL_FWD = "balance_forward", "Balance Forward"

    customer = models.ForeignKey(Newcustomer, on_delete=models.CASCADE, related_name="statements")

    statement_date = models.DateField(default=timezone.now)
    start_date = models.DateField()
    end_date = models.DateField()

    statement_type = models.CharField(
        max_length=32,
        choices=StatementType.choices,
        default=StatementType.TRANSACTION,
    )

    email_to = models.EmailField(blank=True, null=True)

    opening_balance = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0.00"))
    closing_balance = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0.00"))

    memo = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-statement_date", "-id"]
        indexes = [
            models.Index(fields=["company", "statement_date"]),
            models.Index(fields=["company", "customer"]),
        ]

    def __str__(self):
        return f"{self.company.name} | Statement #{self.pk} • {self.customer.customer_name} • {self.statement_date}"


class StatementLine(models.Model):
    class LineKind(models.TextChoices):
        OPENING = "opening_balance", "Opening Balance"
        INVOICE = "invoice", "Invoice"
        PAYMENT = "payment", "Payment"
        SALES_RECEIPT = "sales_receipt", "Sales Receipt"
        BAL_FWD = "balance_forward", "Balance Forward"

    statement = models.ForeignKey(Statement, on_delete=models.CASCADE, related_name="lines")
    date = models.DateField()
    kind = models.CharField(max_length=32, choices=LineKind.choices)

    ref_no = models.CharField(max_length=64, blank=True)
    memo = models.CharField(max_length=255, blank=True)

    amount = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0.00"))
    running_balance = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True, default=Decimal("0.00"))

    source_type = models.CharField(max_length=32, blank=True)
    source_id = models.IntegerField(blank=True, null=True)

    class Meta:
        ordering = ["date", "id"]
        indexes = [
            models.Index(fields=["statement", "date"]),
        ]

    def __str__(self):
        return f"{self.date} • {self.kind} • {self.amount}"


# -------------------------------------------------------------------
# REFUNDS
# -------------------------------------------------------------------

class CustomerRefund(TenantModel):
    customer = models.ForeignKey("sowaf.Newcustomer", on_delete=models.CASCADE, related_name="refunds")
    refund_date = models.DateField(default=timezone.localdate)

    paid_from = models.ForeignKey(
        "accounts.Account",
        on_delete=models.PROTECT,
        related_name="customer_refund_account",
    )
    reference_no = models.CharField(max_length=50, blank=True, null=True)
    memo = models.TextField(blank=True, null=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        indexes = [
            models.Index(fields=["company", "refund_date"]),
            models.Index(fields=["company", "customer"]),
        ]

    def __str__(self):
        return f"{self.company.name} | CustomerRefund {self.id} - {self.customer} ({self.amount})"


class SupplierRefund(TenantModel):
    supplier = models.ForeignKey("sowaf.Newsupplier", on_delete=models.CASCADE, related_name="refunds")
    refund_date = models.DateField(default=timezone.localdate)

    received_to = models.ForeignKey(
        "accounts.Account",
        on_delete=models.PROTECT,
        related_name="supplier_refund_account",
    )
    reference_no = models.CharField(max_length=50, blank=True, null=True)
    memo = models.TextField(blank=True, null=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        indexes = [
            models.Index(fields=["company", "refund_date"]),
            models.Index(fields=["company", "supplier"]),
        ]

    def __str__(self):
        return f"{self.company.name} | SupplierRefund {self.id} - {self.supplier} ({self.amount})"


# -------------------------------------------------------------------
# RECURRING INVOICES (TEMPLATES)
# -------------------------------------------------------------------

class RecurringInvoice(TenantModel):
    FREQ_CHOICES = [
        ("daily", "Daily"),
        ("weekly", "Weekly"),
        ("monthly", "Monthly"),
        ("yearly", "Yearly"),
    ]

    customer = models.ForeignKey(Newcustomer, on_delete=models.CASCADE, related_name="recurring_invoices")
    email = models.EmailField(max_length=255, null=True, blank=True)
    billing_address = models.CharField(max_length=255, null=True, blank=True)
    shipping_address = models.CharField(max_length=255, null=True, blank=True)
    terms = models.CharField(max_length=255, null=True, blank=True)
    sales_rep = models.CharField(max_length=255, null=True, blank=True)

    class_field = models.ForeignKey(Pclass, on_delete=models.CASCADE, null=True, blank=True)
    tags = models.CharField(max_length=255, null=True, blank=True)
    po_num = models.PositiveIntegerField(null=True, blank=True)
    memo = models.CharField(max_length=255, null=True, blank=True)
    customs_notes = models.CharField(max_length=255, null=True, blank=True)

    shipping_fee = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    frequency = models.CharField(max_length=20, choices=FREQ_CHOICES, default="monthly")
    interval = models.PositiveIntegerField(default=1)
    start_date = models.DateField(default=timezone.localdate)
    next_run_date = models.DateField(default=timezone.localdate)

    end_date = models.DateField(null=True, blank=True)
    max_occurrences = models.PositiveIntegerField(null=True, blank=True)
    occurrences_generated = models.PositiveIntegerField(default=0)

    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["company", "is_active", "next_run_date"]),
            models.Index(fields=["company", "customer"]),
        ]

    def __str__(self):
        return f"{self.company.name} | RecurringInvoice#{self.id} - {self.customer.customer_name} ({self.frequency})"


class RecurringInvoiceLine(models.Model):
    recurring = models.ForeignKey(RecurringInvoice, on_delete=models.CASCADE, related_name="lines")
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    description = models.TextField(blank=True, null=True)

    qty = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("1.00"))
    unit_price = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    discount_num = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return f"RecurringLine#{self.id} ({self.product.name})"


class RecurringGeneratedInvoice(models.Model):
    recurring = models.ForeignKey(RecurringInvoice, on_delete=models.CASCADE, related_name="generated")
    invoice = models.ForeignKey(Newinvoice, on_delete=models.CASCADE, related_name="generated_from_recurring")
    run_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("recurring", "run_date")
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["recurring", "run_date"]),
        ]

    def __str__(self):
        return f"Generated Invoice {self.invoice_id} from Recurring {self.recurring_id} on {self.run_date}"


# -------------------------------------------------------------------
# SALES COLUMN PREFERENCES (TENANT SAFE)
# -------------------------------------------------------------------

class ColumnPreference(TenantModel):
    """
    Must be tenant-scoped so the same user can keep different layouts per company.
    """
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    table_name = models.CharField(max_length=80)  # "invoice_list"
    visible_columns = models.JSONField(default=list)
    column_order = models.JSONField(default=list)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("company", "user", "table_name")
        indexes = [
            models.Index(fields=["company", "user", "table_name"]),
        ]

    def __str__(self):
        return f"{self.company.name} - {self.user} - {self.table_name}"