from django.db import models
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import date
from django.contrib.auth.models import User
from accounts.models import Account
from django.utils import timezone

# Create your models here.

class Newcustomer(models.Model):
    logo = models.ImageField(null=True, blank=True)
    customer_name = models.CharField(max_length=255, null=True, blank=True)
    company_name = models.CharField(max_length=255, null=True, blank=True)
    email = models.EmailField(max_length=255, null=True, blank=True)
    phone_number = models.CharField(max_length=10, null=True, blank=True)
    mobile_number = models.CharField(max_length=10, null=True, blank=True)
    website = models.URLField(max_length=255, null=True, blank=True)
    tin_number = models.CharField(max_length=10, null=True, blank=True)
    opening_balance = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True, default=0)
    registration_date = models.DateField(null=True, blank=True)
    street_one = models.CharField(max_length=255, null=True, blank=True)
    street_two = models.CharField(max_length=255, null=True, blank=True)
    city = models.CharField(max_length=255, null=True, blank=True)
    province = models.CharField(max_length=255, null=True, blank=True)
    postal_code = models.CharField(max_length=5, null=True, blank=True)
    country = models.CharField(max_length=255, null=True, blank=True)
    notes = models.TextField(max_length=1000, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    attachments = models.FileField(upload_to='uploads/', null=True, blank=True)
    ar_account = models.OneToOneField(
        Account,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="customer_ar_owner",
    )
    class Meta:
        ordering = ['customer_name']

    def __str__(self):
        return f'{self.customer_name}-{self.company_name}-{self.phone_number}-{self.country}'    
# supplier model
class Newsupplier(models.Model):
    PAYMENT_CHOICES = [
            ('Bank transafer', 'Bank transafer'),
            ('Cheque','Cheque'),
            ('Cash','Cash'),
    ]
    SUPPLIER_CHOICES = [
            ('Goods', 'Goods'),
            ('Services','Services'),
            ('Both','Both'),
    ]
    logo = models.ImageField(null=True, blank=True)
    company_name = models.CharField(max_length=255,null=True, blank=True)
    supplier_type = models.CharField(max_length=255, choices=SUPPLIER_CHOICES, default='',null=True, blank=True)
    contact_person = models.CharField(max_length=255,null=True, blank=True)
    contact_position = models.CharField(max_length=255,null=True, blank=True)
    contact = models.CharField(max_length=10,null=True, blank=True)
    email = models.EmailField(max_length=255,null=True, blank=True)
    open_balance = models.DecimalField(max_digits=10, decimal_places=2, default=0,null=True, blank=True)
    website = models.URLField(max_length=255,null=True, blank=True)
    address1 = models.CharField(max_length=255,null=True, blank=True)
    address2= models.CharField(max_length=255,null=True, blank=True)
    city = models.CharField(max_length=255,null=True, blank=True)
    state = models.CharField(max_length=255,null=True, blank=True)
    zip_code = models.CharField(max_length=5,null=True, blank=True)
    country = models.CharField(max_length=255,null=True, blank=True)
    bank = models.CharField(max_length=255,null=True, blank=True)
    bank_account = models.CharField(max_length=255,null=True, blank=True)
    bank_branch = models.CharField(max_length=255,null=True, blank=True)
    payment_terms = models.CharField(max_length=255,null=True, blank=True)
    currency = models.CharField(max_length=255,null=True, blank=True)
    payment_method = models.CharField(max_length=255, choices=PAYMENT_CHOICES, default='',null=True, blank=True)
    tin = models.CharField(max_length=10,null=True, blank=True)
    reg_number=models.CharField(max_length=255,null=True, blank=True)
    is_active = models.BooleanField(default=True)
    attachments = models.FileField(upload_to='uploads/',null=True, blank=True)
    ap_account = models.OneToOneField(
        Account,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="supplier_ap_owner",
    )
    class Meta:
        ordering =['company_name']

    def __str__(self):
        return f'{self.company_name}-{self.contact_person}-{self.contact}-{self.country}'
    
class Newclient(models.Model):
    CURRENCY_CHOICES = [
        ('UGX', 'UGX'),
        ('USD', 'UGX')
    ]
    INDUSTRY_CHOICES=[
        ('Consumer products','Consumer products'),
        ('Energy and natural resources','Energy and natural resources'),
        ('Financial services','Financial services'),
        ('Healthcare','Healthcare'),
        ('Industrial products','Industrial products'),
        ('Not for profit','Not for profit'),
        ('Individual private clients','Individual private clients'),
        ('Public sector','Public sector'),
        ('Real estate and construction','Real estate and construction'),
        ('Services','Services'),
        ('Technology, media and telecommunications','Technology, media and telecommunications'),
        ('Travel, tourism and leisure','Travel, tourism and leisure'),
        ('Others','Others'),
    ]
    STATUS_CHOICES = [
            ('Active', 'Active'),
            ('Inactive','Inactive'),
    ]
    logo = models.ImageField(null=True, blank=True)
    company = models.CharField(max_length=255,null=True, blank=True)
    phone = models.CharField(max_length=10,null=True, blank=True)
    company_email = models.EmailField(max_length=255,null=True, blank=True)
    address = models.CharField(max_length=255,null=True, blank=True)
    country = models.CharField(max_length=255,null=True, blank=True)
    reg_number=models.CharField(max_length=255,null=True, blank=True)
    start_date = models.DateTimeField(null=True, blank=True)
    contact_name= models.CharField(max_length=255,null=True, blank=True)
    position = models.CharField(max_length=255,null=True, blank=True)
    contact = models.CharField(max_length=10,null=True, blank=True)
    contact_email = models.CharField(max_length=255,null=True, blank=True)
    tin = models.CharField(max_length=10,null=True, blank=True)
    credit_limit = models.DecimalField(max_digits=255, decimal_places=2, default=0,null=True, blank=True)
    payment_terms = models.CharField(max_length=255,null=True, blank=True)
    currency = models.CharField(max_length=255,null=True, blank=True)
    industry = models.CharField(choices=INDUSTRY_CHOICES, default='',null=True, blank=True)
    status = models.CharField(max_length=255, choices=STATUS_CHOICES, default='',null=True, blank=True)
    notes = models.TextField(max_length=1000,null=True, blank=True)
    class Meta:
        ordering =['company']

    def __str__(self):
        return f'{self.company}-{self.contact_name}-{self.contact}-{self.country}'

class Newemployee(models.Model):
    PAYMENT_CHOICES = [
            ('Bank transafer', 'Bank transafer'),
            ('Cheque','Cheque'),
            ('Cash','Cash'),
    ]
    STATUS_CHOICES = [
            ('Active', 'Active'),
            ('Suspended','Suspended'),
            ('Terminated','Terminated'),
    ]
    EMPLOYMENT_CHOICES = [
        ('Full-time', 'Full-time'),
        ('Part-time', 'Part-time'),
        ('Contract', 'Contract'),
        ('Intern', 'Intern'),
        ('Volunteer', 'Volunteer'),
    ]
    GENDER_CHOICES = [
        ('Male', 'Male'),
        ('Female', 'Female'),
        ('Other', 'Other'),
    ]
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    gender = models.CharField(choices=GENDER_CHOICES,default='',max_length=255, null=True, blank=True)
    dob = models.DateField(max_length=255, null=True, blank=True)
    nationality = models.CharField(max_length=255, null=True, blank=True)
    nin_number = models.CharField(max_length=14, null=True, blank=True)
    tin_number= models.CharField(max_length=10, null=True, blank=True)
    profile_picture = models.ImageField(null=True, blank=True)
    phone_number = models.CharField(max_length=10, null=True, blank=True)
    email_address = models.EmailField(max_length=255, null=True, blank=True)
    residential_address = models.CharField(max_length=255, null=True, blank=True)
    emergency_person = models.CharField(max_length=255, null=True, blank=True)
    emergency_contact = models.CharField(max_length=255, null=True, blank=True)
    relationship = models.CharField(max_length=255, null=True, blank=True)
    job_title = models.CharField(max_length=255, null=True, blank=True)
    department = models.CharField(max_length=255, null=True, blank=True)
    employment_type = models.CharField(choices=EMPLOYMENT_CHOICES,default='',max_length=255, null=True, blank=True)
    status = models.CharField(choices=STATUS_CHOICES,default='',max_length=255, null=True, blank=True)
    hire_date = models.DateField(max_length=255, null=True, blank=True)
    supervisor = models.CharField(max_length=255, null=True, blank=True)
    salary = models.DecimalField(max_digits=255,decimal_places=2, default=0, null=True, blank=True)
    payment_frequency = models.CharField(max_length=255, null=True, blank=True)
    payment_method = models.CharField(choices=PAYMENT_CHOICES,default='',max_length=255, null=True, blank=True)
    bank_name = models.CharField(max_length=255, null=True, blank=True)
    bank_account = models.CharField(max_length=255, null=True, blank=True)
    bank_branch = models.CharField(max_length=255, null=True, blank=True)
    nssf_number = models.CharField(max_length=255, null=True, blank=True)
    insurance_provider = models.CharField(max_length=255, null=True, blank=True)
    taxable_allowances = models.DecimalField(max_digits=255,decimal_places=2,default=0, null=True, blank=True)
    intaxable_allowances= models.DecimalField(max_digits=255,decimal_places=2,default=0, null=True, blank=True)
    additional_notes = models.TextField(max_length=1000, null=True, blank=True)
    doc_attachments= models.FileField(upload_to='uploads/')
    
    doc_attachments= models.FileField(upload_to='uploads/')

# assets model
class Newasset(models.Model):
    DEPRECIATION_CHOICES = [
        ('Straight line', 'Straight line'),
        ('Reducing balance', 'Reducing balance'),
    ]
    STATUS_CHOICES = [
        ('Active', 'Active'),
        ('Disposed', 'Disposed'),
        ('Written-Off', 'Written-Off'),
    ]

    asset_name = models.CharField(max_length=255, null=True, blank=True)
    asset_tag = models.CharField(max_length=255, null=True, blank=True)
    asset_category = models.CharField(max_length=255, null=True, blank=True)
    asset_description = models.CharField(max_length=255, null=True, blank=True)
    department = models.CharField(max_length=255, null=True, blank=True)
    custodian = models.CharField(max_length=255, null=True, blank=True)

    asset_status = models.CharField(
        choices=STATUS_CHOICES,
        default='Active',
        max_length=255,
        null=True,
        blank=True
    )

    # Keep as-is (you currently store it as CharField)
    purchase_price = models.CharField(max_length=255, null=True, blank=True)
    purchase_date = models.DateField(null=True, blank=True)

    supplier = models.ForeignKey(Newsupplier, on_delete=models.CASCADE, related_name='supplied_assets')

    warranty = models.DateField(null=True, blank=True)

    funding_source = models.CharField(max_length=255, null=True, blank=True)
    life_span = models.CharField(max_length=255, null=True, blank=True)

    depreciation_method = models.CharField(
        choices=DEPRECIATION_CHOICES,
        default='Straight line',
        max_length=255,
        null=True,
        blank=True
    )

    residual_value = models.DecimalField(decimal_places=2, max_digits=10, null=True, blank=True)
    accumulated_depreciation = models.DecimalField(decimal_places=2, max_digits=10, null=True, blank=True)
    remaining_value = models.DecimalField(decimal_places=2, max_digits=10, null=True, blank=True)

    # asset_account should be a COA account (Fixed Asset account)
    asset_account = models.ForeignKey(
        Account,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="fixed_assets",
        limit_choices_to={"account_type": "NON_CURRENT_ASSET"},
    )

    # payment account (cash/bank) selection
    payment_account = models.ForeignKey(
        Account,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="asset_payments",
        help_text="Bank/Cash account used to pay for the asset",
    )

    capitalization_date = models.DateField(null=True, blank=True)

    cost_center = models.CharField(max_length=255, null=True, blank=True)
    asset_condition = models.CharField(max_length=255, null=True, blank=True)
    maintenance_schedule = models.CharField(max_length=255, null=True, blank=True)
    insurance_details = models.CharField(max_length=255, null=True, blank=True)
    notes = models.CharField(max_length=255, null=True, blank=True)

    asset_attachments = models.FileField(upload_to='uploads/', null=True, blank=True)

    class Meta:
        ordering = ['asset_name']

    def __str__(self):
        return f'{self.asset_name}-{self.asset_category}-{self.department}-{self.custodian}'

    # -----------------------------
    # rs (safe conversions)
    # -----------------------------
    def _D(self, x, default="0.00"):
        try:
            if x is None or x == "":
                return Decimal(default)
            return Decimal(str(x))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal(default)

    def _Q2(self, x: Decimal) -> Decimal:
        return Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def cost(self) -> Decimal:
        return self._Q2(self._D(self.purchase_price, "0.00"))

    def residual(self) -> Decimal:
        return self._Q2(self._D(self.residual_value, "0.00"))

    def life_years(self) -> int:
        try:
            v = int(float(self.life_span or 0))
            return max(v, 0)
        except Exception:
            return 0

    def depreciation_start_date(self) -> date | None:
        return self.capitalization_date or self.purchase_date

    # -----------------------------------------
    # Depreciation (as of a given date)
    # -----------------------------------------
    def depreciation_as_of(self, as_of: date | None = None):
        if as_of is None:
            as_of = timezone.localdate()

        start = self.depreciation_start_date()
        cost = self.cost()
        residual = self.residual()
        life = self.life_years()

        if not start or cost <= 0 or life <= 0:
            entered_acc = self._Q2(self._D(self.accumulated_depreciation, "0.00"))
            nbv = self._Q2(cost - entered_acc)
            if nbv < 0:
                nbv = Decimal("0.00")
            return (entered_acc, nbv)

        if (self.asset_status or "").lower() in {"disposed", "written-off", "written off"}:
            acc = self._Q2(cost - residual if cost > residual else Decimal("0.00"))
            return (acc, Decimal("0.00"))

        if as_of < start:
            return (Decimal("0.00"), cost)

        months = (as_of.year - start.year) * 12 + (as_of.month - start.month)
        months = max(months, 0)

        total_months = life * 12
        months = min(months, total_months)

        base = cost - residual
        if base <= 0:
            return (Decimal("0.00"), cost)

        method = (self.depreciation_method or "").strip().lower()

        # Straight line
        if "straight" in method:
            monthly = base / Decimal(total_months)
            acc = monthly * Decimal(months)
            acc = self._Q2(acc)
            if acc > base:
                acc = self._Q2(base)

            nbv = self._Q2(cost - acc)
            if nbv < residual:
                nbv = residual
            return (acc, nbv)

        # Reducing balance
        if "reducing" in method or "declining" in method:
            if residual <= 0:
                rate = Decimal("2.0") / Decimal(life)  # default to double-declining
                if rate > 1:
                    rate = Decimal("1.0")
            else:
                ratio = residual / cost
                if ratio <= 0:
                    ratio = Decimal("0.0001")
                if ratio >= 1:
                    ratio = Decimal("0.9999")

                rate_float = 1.0 - float(ratio) ** (1.0 / float(life))
                rate = Decimal(str(rate_float))

            monthly_factor_float = (1.0 - float(rate)) ** (float(months) / 12.0)
            nbv = Decimal(str(float(cost) * monthly_factor_float))
            nbv = self._Q2(nbv)

            if nbv < residual:
                nbv = residual

            acc = self._Q2(cost - nbv)
            if acc > base:
                acc = self._Q2(base)
                nbv = self._Q2(cost - acc)

            return (acc, nbv)

        return (Decimal("0.00"), cost)

    def recalc_depreciation_fields(self, as_of: date | None = None):
        acc, nbv = self.depreciation_as_of(as_of=as_of)
        self.accumulated_depreciation = acc
        self.remaining_value = nbv

    def save(self, *args, **kwargs):
        self.recalc_depreciation_fields()
        super().save(*args, **kwargs)
