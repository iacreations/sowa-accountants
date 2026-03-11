# tenancy/models.py
from django.db import models
from django.conf import settings
from django.utils import timezone
from django.contrib.auth.hashers import make_password, check_password
import secrets


class Company(models.Model):
    name = models.CharField(max_length=255)
    company_kind = models.CharField(
        max_length=20,
        choices=[
            ("SOWA", "Sowa"),
            ("CLIENT", "Client"),
            ],
        default="CLIENT"       
    )
    logo = models.ImageField(upload_to="company_logos/", null=True, blank=True)

    email = models.EmailField(null=True, blank=True)
    phone = models.CharField(max_length=20, null=True, blank=True)

    address = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    business_reg_no = models.CharField(max_length=100, null=True, blank=True)
    business_start_date = models.DateField(null=True, blank=True)

    contact_name = models.CharField(max_length=255, null=True, blank=True)
    contact_position = models.CharField(max_length=255, null=True, blank=True)
    contact_phone = models.CharField(max_length=20, null=True, blank=True)
    contact_email = models.EmailField(null=True, blank=True)

    tax_id = models.CharField(max_length=100, null=True, blank=True)
    credit_limit = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    payment_terms = models.CharField(max_length=100, null=True, blank=True)

    currency = models.CharField(max_length=10, default="UGX")

    industry = models.CharField(max_length=150, null=True, blank=True)
    notes = models.TextField(null=True, blank=True)

    billing_cycle = models.CharField(max_length=20, default="monthly")  # monthly/annual
    bill_to = models.CharField(max_length=20, default="firm")          # firm/client
    primary_admin = models.CharField(max_length=20, null=True, blank=True)  # client/sowa

    payment_method = models.CharField(max_length=50, null=True, blank=True)
    payment_reference = models.CharField(max_length=255, null=True, blank=True)

    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="created_companies",
    )

    def __str__(self):
        return self.name


class Subscription(models.Model):
    PLAN_CHOICES = [
        ("BASIC", "Basic"),
        ("PRO", "Pro"),
        ("ENTERPRISE", "Enterprise"),
    ]

    STATUS_CHOICES = [
        ("TRIAL", "Trial"),
        ("ACTIVE", "Active"),
        ("EXPIRED", "Expired"),
        ("SUSPENDED", "Suspended"),
    ]

    company = models.OneToOneField(Company, on_delete=models.CASCADE, related_name="subscription")
    plan = models.CharField(max_length=20, choices=PLAN_CHOICES, default="BASIC")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="TRIAL")

    start_date = models.DateField(default=timezone.now)
    end_date = models.DateField(null=True, blank=True)

    max_users = models.PositiveIntegerField(default=3)

    def __str__(self):
        return f"{self.company.name} - {self.plan} ({self.status})"


class CompanyMember(models.Model):
    """
    This is your ROLE system.
    We map roles -> allowed MODULES (sales/expenses/inventory/accounts/reports/settings).
    """
    ROLE_CHOICES = [
        ("OWNER", "Owner"),
        ("MANAGER", "Manager"),
        ("ACCOUNTANT", "Accountant"),
        ("SALES", "Sales"),
        ("INVENTORY", "Inventory"),
        ("VIEWER", "Viewer"),
    ]

    company = models.ForeignKey(Company, on_delete=models.CASCADE, related_name="members")
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="company_memberships")

    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default="VIEWER")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("company", "user")

    def __str__(self):
        return f"{self.user.username} -> {self.company.name} ({self.role})"

    @staticmethod
    def role_modules(role: str) -> set:
        """
        Define module access here. You can tweak anytime.
        """
        role = (role or "").upper()

        if role == "OWNER":
            return {"home", "sales", "expenses", "inventory", "accounts", "reports", "settings"}

        if role == "MANAGER":
            return {"home", "sales", "expenses", "inventory", "accounts", "reports", "settings"}

        if role == "ACCOUNTANT":
            return {"home", "expenses", "accounts", "reports"}

        if role == "SALES":
            return {"home", "sales"}

        if role == "INVENTORY":
            return {"home", "inventory"}

        # VIEWER
        return {"home"}

    def can_access(self, module_key: str) -> bool:
        return module_key in self.role_modules(self.role)


class EmailOTP(models.Model):
    PURPOSE_CHOICES = [
        ("LOGIN", "Login"),
    ]

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="email_otps",
        null=True,
        blank=True
    )
    email = models.EmailField()
    purpose = models.CharField(max_length=20, choices=PURPOSE_CHOICES, default="LOGIN")

    otp_hash = models.CharField(max_length=255)
    attempts = models.PositiveIntegerField(default=0)
    max_attempts = models.PositiveIntegerField(default=5)

    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()

    is_used = models.BooleanField(default=False)

    last_sent_at = models.DateTimeField(null=True, blank=True)
    send_count = models.PositiveIntegerField(default=0)

    @staticmethod
    def generate_code(length=6) -> str:
        digits = "0123456789"
        return "".join(secrets.choice(digits) for _ in range(length))

    def set_code(self, raw_code: str):
        self.otp_hash = make_password(raw_code)

    def check_code(self, raw_code: str) -> bool:
        return check_password(raw_code, self.otp_hash)

    def is_expired(self) -> bool:
        return timezone.now() >= self.expires_at

    def can_attempt(self) -> bool:
        return (not self.is_used) and (not self.is_expired()) and (self.attempts < self.max_attempts)

    def __str__(self):
        return f"{self.email} OTP ({self.purpose})"


class CompanyInvite(models.Model):
    """
    Invite a user into a company.
    Owner/Manager can invite. SOWA staff can invite too.
    Link takes them to OTP login (prefilled email).
    """
    company = models.ForeignKey(Company, on_delete=models.CASCADE, related_name="invites")
    email = models.EmailField()
    role = models.CharField(max_length=20, choices=CompanyMember.ROLE_CHOICES, default="VIEWER")

    token = models.CharField(max_length=64, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    used_at = models.DateTimeField(null=True, blank=True)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="created_company_invites",
    )

    @staticmethod
    def _new_token():
        return secrets.token_urlsafe(32)

    @classmethod
    def create(cls, company, email, role="VIEWER", hours=24, created_by=None):
        now = timezone.now()
        return cls.objects.create(
            company=company,
            email=email,
            role=role,
            token=cls._new_token(),
            expires_at=now + timezone.timedelta(hours=hours),
            created_by=created_by,
        )

    def is_valid(self) -> bool:
        if self.used_at:
            return False
        return timezone.now() < self.expires_at

    def __str__(self):
        return f"Invite {self.email} -> {self.company.name} ({self.role})"