from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone
from django.contrib.auth.hashers import make_password, check_password
import secrets


class Newuser(AbstractUser):
    contact = models.CharField(max_length=15, blank=True, null=True, unique=True)
    can_manage_staff = models.BooleanField(default=False)
    session_key = models.CharField(max_length=40, blank=True, null=True)

    def __str__(self):
        return f"user- {self.username} | email- {self.email} | contact- {self.contact}"


class StaffInvite(models.Model):
    email = models.EmailField(unique=True)
    token = models.CharField(max_length=64, unique=True)
    full_name = models.CharField(max_length=255, blank=True, null=True)
    role_label = models.CharField(max_length=100, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    used_at = models.DateTimeField(blank=True, null=True)

    created_by = models.ForeignKey(
        "sowaAuth.Newuser",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="created_staff_invites"
    )

    @staticmethod
    def _new_token():
        return secrets.token_urlsafe(32)

    @classmethod
    def create_invite(cls, email, hours=72, created_by=None, full_name=None, role_label=None):
        now = timezone.now()
        obj, _created = cls.objects.update_or_create(
            email=email,
            defaults={
                "token": cls._new_token(),
                "expires_at": now + timezone.timedelta(hours=hours),
                "used_at": None,
                "created_by": created_by,
                "full_name": full_name,
                "role_label": role_label,
            }
        )
        return obj

    def is_valid(self):
        if self.used_at:
            return False
        return timezone.now() < self.expires_at

    def __str__(self):
        return f"Staff Invite -> {self.email}"


class StaffLoginOTP(models.Model):
    PURPOSE_CHOICES = [
        ("STAFF_LOGIN", "Staff Login"),
    ]

    user = models.ForeignKey(
        "sowaAuth.Newuser",
        on_delete=models.CASCADE,
        related_name="staff_login_otps"
    )
    email = models.EmailField()
    purpose = models.CharField(max_length=30, choices=PURPOSE_CHOICES, default="STAFF_LOGIN")

    otp_hash = models.CharField(max_length=255)
    attempts = models.PositiveIntegerField(default=0)
    max_attempts = models.PositiveIntegerField(default=5)

    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()

    is_used = models.BooleanField(default=False)
    last_sent_at = models.DateTimeField(blank=True, null=True)
    send_count = models.PositiveIntegerField(default=0)

    @staticmethod
    def generate_code(length=6):
        digits = "0123456789"
        return "".join(secrets.choice(digits) for _ in range(length))

    def set_code(self, raw_code):
        self.otp_hash = make_password(raw_code)

    def check_code(self, raw_code):
        return check_password(raw_code, self.otp_hash)

    def is_expired(self):
        return timezone.now() >= self.expires_at

    def can_attempt(self):
        return (not self.is_used) and (not self.is_expired()) and (self.attempts < self.max_attempts)

    def __str__(self):
        return f"{self.email} Staff OTP"