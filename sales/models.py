from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from datetime import timedelta
from decimal import Decimal
from django.db.models import Sum
from accounts.models import Account
from sowaf.models import Newcustomer
from inventory.models import Product,Pclass
# Create your models here.


class Newinvoice(models.Model):
    date_created = models.DateField(null=True, blank=True)
    due_date = models.DateField(null=True, blank=True)
    customer = models.ForeignKey(Newcustomer, on_delete=models.CASCADE)
    email = models.EmailField(max_length=255, null=True, blank=True)
    billing_address = models.CharField(max_length=255, null=True, blank=True)
    shipping_address = models.CharField(max_length=255, null=True, blank=True)
    terms = models.CharField(max_length=255, null=True, blank=True)
    sales_rep = models.CharField(max_length=255, null=True, blank=True)
    class_field = models.ForeignKey(Pclass, on_delete=models.CASCADE)
    tags = models.CharField(max_length=255, null=True, blank=True)
    po_num = models.PositiveIntegerField(null=True, blank=True)
    memo = models.CharField(max_length=255, null=True, blank=True)
    customs_notes = models.CharField(max_length=255, null=True, blank=True)
    attachments = models.FileField(null=True, blank=True)
    subtotal = models.FloatField(default=0)
    total_discount = models.FloatField(default=0)
    shipping_fee = models.FloatField(default=0)
    total_vat = models.FloatField(default=0)
    total_due = models.FloatField(default=0)

    class Meta:
        ordering =['date_created']


    def __str__(self):
        return f'Customer={self.customer.customer_name} | date created - {self.date_created} | due date - {self.due_date} | sales representative - {self.sales_rep}'
    
    @property
    def amount_paid(self):
        # sum of all payments applied to this invoice
        return self.payments_applied.aggregate(total=Sum("amount_paid"))["total"] or 0

    @property
    def balance(self):
        # remaining amount to be paid
        return self.total_due - self.amount_paid
    
    
class InvoiceItem(models.Model):
    invoice = models.ForeignKey(Newinvoice, on_delete=models.CASCADE,related_name='items')
    # FK to product, but allow custom lines
    product = models.ForeignKey(Product, on_delete=models.CASCADE)

    # 🔒 snapshots to preserve history
    name_snapshot = models.CharField(max_length=255, blank=True)   # product name at sale time
    description = models.TextField(blank=True, null=True)
    # income_account = models.ForeignKey(  # account used for posting this line
    #     Account, on_delete=models.CASCADE,
    #     limit_choices_to={'type': 'Income'},
    # )

    # quantities & money as decimals
    qty = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("1.00"))
    unit_price = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    vat = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)  # e.g. 18.00 for 18%
    amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    discount_num = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)
    discount_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)

    def save(self, *args, **kwargs):
        # fill snapshots from product if not provided
        if self.product and not self.name_snapshot:
            self.name_snapshot = self.product.name

        # compute line_total
        self.line_total = (self.qty or 0) * (self.unit_price or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        label = self.name_snapshot or (self.product.name if self.product else "Custom line")
        return f"{label} x {self.qty} (Invoice {self.invoice_id})"
       
# Payment model

class Payment(models.Model):
    PAYMENT_METHODS = [
        ("cash", "Cash"),
        ("bank_transfer", "Bank Transfer"),
        ("mobile_money", "Mobile Money"),
        ("cheque", "Cheque"),
    ]

    customer = models.ForeignKey(Newcustomer, on_delete=models.CASCADE, related_name="payments")
    payment_date = models.DateField()
    payment_method = models.CharField(max_length=50, choices=PAYMENT_METHODS)
    deposit_to = models.ForeignKey(Account, on_delete=models.CASCADE, limit_choices_to={'account_type__in':['Bank','Cash and Cash Equivalents']}, related_name='payment_account')
    reference_no = models.CharField(max_length=50, blank=True, null=True)
    tags = models.CharField(max_length=255, blank=True, null=True)
    memo = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"Payment {self.id} - {self.customer.customer_name}"


class PaymentInvoice(models.Model):
    payment = models.ForeignKey(Payment, on_delete=models.CASCADE, related_name="applied_invoices")
    invoice = models.ForeignKey("Newinvoice", on_delete=models.CASCADE, related_name="payments_applied")
    amount_paid = models.DecimalField(max_digits=12, decimal_places=2)

    def __str__(self):
        return f"Payment {self.payment.id} → Invoice {self.invoice.id} ({self.amount_paid})"

# working on the receipt
class SalesReceipt(models.Model):
    PAYMENT_METHODS = [
        ("cash", "Cash"),
        ("bank_transfer", "Bank Transfer"),
        ("mobile_money", "Mobile Money"),
        ("cheque", "Cheque"),
    ]

    customer      = models.ForeignKey(Newcustomer, on_delete=models.CASCADE, related_name="sales_receipts")
    receipt_date  = models.DateField(default=timezone.now)
    payment_method= models.CharField(max_length=50, choices=PAYMENT_METHODS, default="cash")
    deposit_to    = models.ForeignKey(Account, on_delete=models.CASCADE, related_name="sales_receipts")
    reference_no  = models.CharField(max_length=50, blank=True, null=True)
    tags          = models.CharField(max_length=255, blank=True, null=True)
    memo          = models.TextField(blank=True, null=True)

    # totals (for the doc)
    subtotal       = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    total_discount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    total_vat      = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    shipping_fee   = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    total_amount   = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    amount_paid = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    balance     = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-receipt_date", "-id"]

    def __str__(self):
        return f"Sales Receipt {self.id} - from {self.customer.customer_name}"
    
class SalesReceiptLine(models.Model):
    receipt      = models.ForeignKey(SalesReceipt, on_delete=models.CASCADE, related_name="lines")
    product      = models.ForeignKey(Product, on_delete=models.CASCADE, null=True, blank=True)
    description  = models.CharField(max_length=255, blank=True, null=True)
    qty          = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    unit_price   = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    amount       = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    # optional per-line splits (we still keep header % too)
    discount_pct = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("0.00"))
    discount_amt = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    vat_amt      = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    def __str__(self):
        return f"SR#{self.receipt_id} - {self.description or (self.product and self.product.name) or 'Line'}"

# customer statements

class Statement(models.Model):
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

    # Totals captured at save time (snapshot, not live)
    opening_balance = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0.00"))
    closing_balance = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0.00"))

    memo = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Statement #{self.pk} • {self.customer.customer_name} • {self.statement_date}"


class StatementLine(models.Model):
    class LineKind(models.TextChoices):
        OPENING = "opening_balance", "Opening Balance"
        INVOICE = "invoice", "Invoice"
        PAYMENT = "payment", "Payment"
        SALES_RECEIPT = "sales_receipt", "Sales Receipt"
        BAL_FWD = "balance_forward", "Balance Forward"   # used by Balance Forward format

    statement = models.ForeignKey(Statement, on_delete=models.CASCADE, related_name="lines")
    date = models.DateField()
    kind = models.CharField(max_length=32, choices=LineKind.choices)
    ref_no = models.CharField(max_length=64, blank=True)
    memo = models.CharField(max_length=255, blank=True)

    # Positive = charge, Negative = credit
    amount = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0.00"))
    running_balance = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True, default=Decimal("0.00"))

    # Optional link back to source objects
    source_type = models.CharField(max_length=32, blank=True)
    source_id = models.IntegerField(blank=True, null=True)

    class Meta:
        ordering = ["date", "id"]

    def __str__(self):
        return f"{self.date} • {self.kind} • {self.amount}"
