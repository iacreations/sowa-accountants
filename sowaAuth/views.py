from django.conf import settings
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout, get_user_model
from django.contrib.auth.decorators import login_required
from django.core.mail import send_mail
from django.db.models import Q
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from tenancy.models import CompanyMember
from .models import StaffInvite, StaffLoginOTP

User = get_user_model()

STAFF_LOGIN_OTP_TTL_MINUTES = 5
STAFF_LOGIN_OTP_RESEND_COOLDOWN_SECONDS = 45


def sowa_staff_admin_required(view_func):
    def _wrapped(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect("sowaAuth:login")

        if not (request.user.is_superuser or getattr(request.user, "can_manage_staff", False)):
            messages.error(request, "Not allowed.")
            return redirect("sowaf:home")

        return view_func(request, *args, **kwargs)
    return _wrapped


def _send_staff_invite_email(request, invite: StaffInvite):
    link = request.build_absolute_uri(reverse("sowaAuth:accept_invite", args=[invite.token]))
    subject = "YoAccountant - Sowa Staff Invite"
    message = (
        f"Hello,\n\n"
        f"You have been invited to join Sowa staff on YoAccountant.\n\n"
        f"Use this link to set your password and activate your account:\n{link}\n\n"
        f"This link expires in 72 hours.\n\n"
        f"Regards,\nYoAccountant"
    )
    send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, [invite.email], fail_silently=False)


def _send_staff_login_otp_email(to_email: str, code: str):
    subject = "Your YoAccountant staff login OTP"
    message = (
        f"Your Sowa staff login OTP is: {code}\n\n"
        f"This OTP expires in {STAFF_LOGIN_OTP_TTL_MINUTES} minutes.\n"
        f"If you did not request this login, ignore this email."
    )
    send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, [to_email], fail_silently=False)


def auth_page(request):
    return render(request, "registration/auth.html")


@require_http_methods(["GET", "POST"])
def accept_staff_invite(request, token):
    invite = StaffInvite.objects.filter(token=token).first()

    if not invite or not invite.is_valid():
        messages.error(request, "Invite link invalid or expired.")
        return redirect("sowaAuth:login")

    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        email = (request.POST.get("email") or "").strip().lower()
        contact = (request.POST.get("contact") or "").strip()
        password = request.POST.get("password") or ""
        confirm_password = request.POST.get("confirm_password") or ""

        has_error = False

        if email != (invite.email or "").lower():
            has_error = True
            messages.error(request, "This invite is only valid for the invited email.")

        if not username:
            has_error = True
            messages.error(request, "Name is required.")

        existing_other_username = (
            User.objects
            .filter(username__iexact=username)
            .exclude(email__iexact=email)
            .exists()
        )
        if existing_other_username:
            has_error = True
            messages.error(request, "Username already exists.")

        existing_other_contact = False
        if contact:
            existing_other_contact = (
                User.objects
                .filter(contact__iexact=contact)
                .exclude(email__iexact=email)
                .exists()
            )
        if existing_other_contact:
            has_error = True
            messages.error(request, "Phone number already exists.")

        if len(password) < 8:
            has_error = True
            messages.error(request, "Password must be at least 8 characters.")

        if password != confirm_password:
            has_error = True
            messages.error(request, "Password does not match.")

        if has_error:
            return render(request, "registration/accept_staff_invite.html", {
                "invite": invite,
                "prefill_username": username,
                "prefill_contact": contact,
            })

        user = User.objects.filter(email__iexact=email).first()

        if user:
            user.username = username
            user.contact = contact if contact else None
            user.email = email
            user.is_staff = True
            user.is_active = True
            user.set_password(password)
            user.save()
        else:
            user = User.objects.create_user(
                username=username,
                email=email,
                password=password,
            )
            user.contact = contact if contact else None
            user.is_staff = True
            user.is_active = True
            user.save()

        invite.used_at = timezone.now()
        invite.save(update_fields=["used_at"])

        messages.success(request, "Staff account activated. Please log in with email, password and OTP.")
        return redirect("sowaAuth:login")

    return render(request, "registration/accept_staff_invite.html", {"invite": invite})


@require_http_methods(["POST"])
def login_user(request):
    identifier = (request.POST.get("username") or "").strip()
    password = request.POST.get("password") or ""

    user = authenticate(request, username=identifier, password=password)

    if user is None:
        candidate = User.objects.filter(
            Q(username__iexact=identifier) |
            Q(email__iexact=identifier) |
            Q(contact__iexact=identifier)
        ).first()

        if candidate:
            user = authenticate(request, username=candidate.username, password=password)

    if user is None:
        messages.error(request, "Invalid credentials")
        return redirect("sowaAuth:login")

    # -------------------------------------------------
    # STAFF / SOWA LOGIN FLOW => OTP FIRST
    # -------------------------------------------------
    if user.is_staff or user.is_superuser:
        email = (user.email or "").strip().lower()
        if not email:
            messages.error(request, "This staff account has no email. Contact admin.")
            return redirect("sowaAuth:login")

        last = StaffLoginOTP.objects.filter(
            user=user,
            email__iexact=email,
            purpose="STAFF_LOGIN",
            is_used=False
        ).order_by("-created_at").first()

        now = timezone.now()

        if last and last.last_sent_at:
            seconds_since = (now - last.last_sent_at).total_seconds()
            if seconds_since < STAFF_LOGIN_OTP_RESEND_COOLDOWN_SECONDS:
                wait = int(STAFF_LOGIN_OTP_RESEND_COOLDOWN_SECONDS - seconds_since)
                messages.error(request, f"Please wait {wait}s then try again.")
                return redirect("sowaAuth:login")

        code = StaffLoginOTP.generate_code(6)

        otp = StaffLoginOTP.objects.create(
            user=user,
            email=email,
            purpose="STAFF_LOGIN",
            expires_at=now + timezone.timedelta(minutes=STAFF_LOGIN_OTP_TTL_MINUTES),
            last_sent_at=now,
            send_count=1,
        )
        otp.set_code(code)
        otp.save()

        _send_staff_login_otp_email(email, code)

        request.session["staff_login_user_id"] = user.id
        request.session["staff_login_otp_id"] = otp.id
        request.session["staff_login_email"] = email

        messages.success(request, f"OTP sent to {email}.")
        return redirect("sowaAuth:staff_otp_verify")

    # -------------------------------------------------
    # CLIENT LOGIN FLOW => DIRECT LOGIN
    # -------------------------------------------------
    login(request, user)
    messages.success(request, "Login successful")

    memberships = CompanyMember.objects.filter(
        user=user,
        is_active=True,
        company__is_active=True
    ).select_related("company")

    request.session["workspace_mode"] = "client"

    if memberships.count() == 1:
        request.session["active_company_id"] = memberships.first().company_id
        request.session.modified = True
        return redirect("sowaf:home")

    request.session["active_company_id"] = None
    request.session.modified = True
    return redirect("tenancy:choose_company")


@require_http_methods(["GET", "POST"])
def staff_otp_verify(request):
    user_id = request.session.get("staff_login_user_id")
    otp_id = request.session.get("staff_login_otp_id")
    email = request.session.get("staff_login_email")

    if not user_id or not otp_id or not email:
        messages.error(request, "Login session expired. Please log in again.")
        return redirect("sowaAuth:login")

    user = User.objects.filter(id=user_id, email__iexact=email).first()
    otp = StaffLoginOTP.objects.filter(
        id=otp_id,
        user_id=user_id,
        email__iexact=email,
        purpose="STAFF_LOGIN"
    ).first()

    if not user or not otp:
        messages.error(request, "OTP session not found. Please log in again.")
        return redirect("sowaAuth:login")

    if request.method == "POST":
        code = (request.POST.get("otp") or "").strip()

        if not code:
            messages.error(request, "Enter the OTP.")
            return render(request, "registration/staff_otp_verify.html", {"email": email})

        if not otp.can_attempt():
            messages.error(request, "OTP expired / used / too many attempts. Please log in again.")
            return redirect("sowaAuth:login")

        otp.attempts += 1
        otp.save(update_fields=["attempts"])

        if not otp.check_code(code):
            remaining = max(0, otp.max_attempts - otp.attempts)
            messages.error(request, f"Invalid OTP. Attempts left: {remaining}.")
            return render(request, "registration/staff_otp_verify.html", {"email": email})

        otp.is_used = True
        otp.save(update_fields=["is_used"])

        login(request, user, backend="sowaAuth.backends.UsernameEmailPhoneBackend")

        # IMPORTANT:
        # Sowa workspace should NOT set a client company.
        request.session["active_company_id"] = None
        request.session["workspace_mode"] = "sowa"

        request.session.pop("staff_login_user_id", None)
        request.session.pop("staff_login_otp_id", None)
        request.session.pop("staff_login_email", None)
        request.session.modified = True

        messages.success(request, "Login successful.")
        return redirect("sowaf:home")

    return render(request, "registration/staff_otp_verify.html", {"email": email})


@login_required
@sowa_staff_admin_required
def staff_users(request):
    staff_members = User.objects.filter(is_staff=True).order_by("-is_superuser", "username")
    invites = StaffInvite.objects.order_by("-created_at")

    return render(request, "registration/sowa_staff_users.html", {
        "staff_members": staff_members,
        "invites": invites,
    })


@login_required
@sowa_staff_admin_required
@require_http_methods(["POST"])
def staff_invite_create(request):
    email = (request.POST.get("email") or "").strip().lower()
    full_name = (request.POST.get("full_name") or "").strip()
    role_label = (request.POST.get("role_label") or "").strip()

    if not email:
        messages.error(request, "Email is required.")
        return redirect("sowaAuth:staff_users")

    invite = StaffInvite.create_invite(
        email=email,
        created_by=request.user,
        full_name=full_name,
        role_label=role_label,
    )

    try:
        _send_staff_invite_email(request, invite)
        messages.success(request, f"Staff invite sent to {email}.")
    except Exception:
        messages.error(request, "Invite created but email failed. Check email settings.")

    return redirect("sowaAuth:staff_users")


@login_required
@sowa_staff_admin_required
@require_http_methods(["POST"])
def staff_toggle_active(request, user_id):
    member = get_object_or_404(User, id=user_id, is_staff=True)

    if member.id == request.user.id and member.is_superuser:
        messages.error(request, "You cannot deactivate your own superuser account.")
        return redirect("sowaAuth:staff_users")

    member.is_active = not member.is_active
    member.save(update_fields=["is_active"])

    if member.is_active:
        messages.success(request, f"{member.username} activated.")
    else:
        messages.success(request, f"{member.username} deactivated.")

    return redirect("sowaAuth:staff_users")


@login_required
@sowa_staff_admin_required
@require_http_methods(["POST"])
def staff_toggle_manager(request, user_id):
    member = get_object_or_404(User, id=user_id, is_staff=True)

    if member.is_superuser:
        messages.error(request, "Superuser is already above staff manager level.")
        return redirect("sowaAuth:staff_users")

    member.can_manage_staff = not member.can_manage_staff
    member.save(update_fields=["can_manage_staff"])

    if member.can_manage_staff:
        messages.success(request, f"{member.username} can now manage staff.")
    else:
        messages.success(request, f"{member.username} can no longer manage staff.")

    return redirect("sowaAuth:staff_users")


def logout_user(request):
    request.session.pop("active_company_id", None)
    request.session.pop("workspace_mode", None)
    request.session.pop("staff_login_user_id", None)
    request.session.pop("staff_login_otp_id", None)
    request.session.pop("staff_login_email", None)

    logout(request)
    return redirect("sowaAuth:login")