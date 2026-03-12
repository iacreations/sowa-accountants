from decimal import Decimal, InvalidOperation

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model, login
from django.contrib.auth.decorators import login_required
from django.core.mail import send_mail
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from .models import Company, CompanyMember, Subscription, EmailOTP, CompanyInvite
from .permissions import module_required, role_required, get_membership

User = get_user_model()

OTP_TTL_MINUTES = 5
OTP_RESEND_COOLDOWN_SECONDS = 45


def staff_only(view_func):
    def _wrapped(request, *args, **kwargs):
        if not (request.user.is_staff or request.user.is_superuser):
            messages.error(request, "Not allowed.")
            return redirect("sowaf:home")
        return view_func(request, *args, **kwargs)
    return _wrapped


def _to_decimal(val):
    if val is None:
        return None
    s = str(val).replace(",", "").strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _plan_from_pkg(selected_package: str) -> str:
    pkg = (selected_package or "").lower().strip()
    if pkg in ("trial", "starter"):
        return "BASIC"
    if pkg == "pro":
        return "PRO"
    return "ENTERPRISE"


def _max_users_for_plan(plan: str) -> int:
    if plan == "BASIC":
        return 3
    if plan == "PRO":
        return 10
    return 50


def _safe_parse_iso_datetime(dt_str: str):
    if not dt_str:
        return None
    try:
        s = dt_str.replace("Z", "+00:00")
        dt = timezone.datetime.fromisoformat(s)
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.get_current_timezone())
        return dt
    except Exception:
        return None


def _get_sowa_company():
    return Company.objects.filter(company_kind="SOWA", is_active=True).order_by("id").first()


def _send_invite_email(request, invite: CompanyInvite):
    link = request.build_absolute_uri(reverse("tenancy:client_invite", args=[invite.token]))
    subject = "YoAccountant - Your access link"
    message = (
        f"Hello,\n\n"
        f"You have been invited to access YoAccountant for: {invite.company.name}\n\n"
        f"Click this link to start login (OTP):\n{link}\n\n"
        f"This link expires in 24 hours.\n\n"
        f"Regards,\nYoAccountant"
    )
    send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, [invite.email], fail_silently=False)


def _send_otp_email(to_email: str, code: str):
    subject = "Your YoAccountant login code"
    message = (
        f"Your login code is: {code}\n\n"
        f"This code expires in {OTP_TTL_MINUTES} minutes.\n"
        f"If you didn’t request this, ignore this email."
    )
    send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, [to_email], fail_silently=False)


def _get_client_user(email: str):
    email = (email or "").strip().lower()
    if not email:
        return None

    user = User.objects.filter(email__iexact=email).first()
    if not user:
        return None

    is_member = CompanyMember.objects.filter(
        user=user,
        is_active=True,
        company__is_active=True
    ).exists()

    return user if is_member else None


def _get_or_create_user_for_email(email: str):
    email = (email or "").strip().lower()
    if not email:
        return None

    user = User.objects.filter(email__iexact=email).first()
    if user:
        return user

    base = (email.split("@")[0] or "client")[:25]
    username = base
    i = 1
    while User.objects.filter(username__iexact=username).exists():
        i += 1
        username = f"{base}{i}"

    user = User.objects.create_user(username=username, email=email, password=None)
    user.set_unusable_password()
    user.save()
    return user


def _set_default_company_in_session(request, user):
    """
    Since each client belongs to only one company:
    - auto-pick the user's active company
    - no chooser needed for clients
    """
    owner_membership = (
        CompanyMember.objects
        .filter(user=user, is_active=True, role="OWNER", company__is_active=True)
        .select_related("company")
        .order_by("-created_at")
        .first()
    )

    if owner_membership:
        request.session["company_id"] = owner_membership.company_id
        request.session["workspace_mode"] = "client"
        return owner_membership.company

    membership = (
        CompanyMember.objects
        .filter(user=user, is_active=True, company__is_active=True)
        .select_related("company")
        .order_by("-created_at")
        .first()
    )

    if membership:
        request.session["company_id"] = membership.company_id
        request.session["workspace_mode"] = "client"
        return membership.company

    return None


@login_required
def choose_company(request):
    """
    Silent mode:
    - Clients do not choose a company anymore.
    - Staff can still use this route, but we redirect them intelligently.
    """
    if request.user.is_staff or request.user.is_superuser:
        company_id = request.session.get("company_id")
        if company_id:
            return redirect("sowaf:home")

        sowa_company = _get_sowa_company()
        if sowa_company:
            request.session["company_id"] = sowa_company.id
            request.session["workspace_mode"] = "sowa"

        return redirect("sowaf:home")

    company = _set_default_company_in_session(request, request.user)
    if not company:
        messages.error(request, "You are not assigned to any company yet.")
        return redirect("sowaAuth:login")

    return redirect("sowaf:home")


@login_required
def switch_company(request, company_id):
    company = get_object_or_404(Company, id=company_id, is_active=True)

    if request.user.is_staff or request.user.is_superuser:
        request.session["company_id"] = company.id
        request.session["workspace_mode"] = "sowa" if company.company_kind == "SOWA" else "client"
        messages.success(request, f"Switched to {company.name}")
        return redirect("sowaf:home")

    is_member = CompanyMember.objects.filter(
        company=company,
        user=request.user,
        is_active=True
    ).exists()

    if not is_member:
        messages.error(request, "You are not allowed to access that company.")
        company = _set_default_company_in_session(request, request.user)
        if company:
            return redirect("sowaf:home")
        return redirect("sowaAuth:login")

    request.session["company_id"] = company.id
    request.session["workspace_mode"] = "client"
    messages.success(request, f"Switched to {company.name}")
    return redirect("sowaf:home")


@login_required
def exit_company(request):
    """
    For SOWA staff: return to Sowa Accountant workspace.
    """
    if not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "Not allowed.")
        return redirect("sowaf:home")

    sowa_company = _get_sowa_company()
    if not sowa_company:
        messages.error(request, "SOWA company not configured.")
        return redirect("sowaf:home")

    request.session["company_id"] = sowa_company.id
    request.session["workspace_mode"] = "sowa"

    messages.success(request, "Returned to Sowa Accountant.")
    return redirect("sowaf:home")


@login_required
@staff_only
def company_list(request):
    companies = (
        Company.objects
        .filter(is_active=True, company_kind="CLIENT")
        .select_related("subscription")
        .order_by("-created_at")
    )
    return render(request, "Clients.html", {"companies": companies})


@login_required
@staff_only
@transaction.atomic
def company_create(request):
    """
    FREE TRIAL + AUTO INVITE OWNER (company email).
    """
    if request.method == "POST":
        company_name = (request.POST.get("company_name") or "").strip()
        company_phone = (request.POST.get("company_phone") or "").strip()
        company_email = (request.POST.get("company_email") or "").strip()
        company_address = (request.POST.get("company_address") or "").strip()
        company_country = (request.POST.get("company_country") or "").strip()

        business_reg_no = (request.POST.get("business_reg_no") or "").strip() or None
        business_start_date = request.POST.get("business_start_date") or None

        contact_name = (request.POST.get("contact_name") or "").strip()
        contact_position = (request.POST.get("contact_position") or "").strip()
        contact_phone = (request.POST.get("contact_phone") or "").strip()
        contact_email = (request.POST.get("contact_email") or "").strip()

        tax_id = (request.POST.get("tax_id") or "").strip() or None
        credit_limit = _to_decimal(request.POST.get("credit_limit"))
        payment_terms = (request.POST.get("payment_terms") or "").strip() or None

        industry = (request.POST.get("industry") or "").strip()
        notes = (request.POST.get("notes") or "").strip() or None

        selected_package = (request.POST.get("selected_package") or "trial").strip()
        billing_cycle = (request.POST.get("billing_cycle") or "monthly").strip()
        bill_to = (request.POST.get("bill_to") or "firm").strip()
        primary_admin = (request.POST.get("primary_admin") or "client").strip()

        trial_starts_at = (request.POST.get("trial_starts_at") or "").strip()
        trial_ends_at = (request.POST.get("trial_ends_at") or "").strip()

        required = [
            company_name, company_phone, company_email, company_address, company_country,
            contact_name, contact_position, contact_phone, contact_email,
            industry, selected_package, billing_cycle, bill_to, primary_admin,
        ]
        if any(not str(x).strip() for x in required):
            messages.error(request, "Please fill all required fields.")
            return render(request, "Clients_form.html")

        company = Company.objects.create(
            name=company_name,
            company_kind="CLIENT",
            logo=request.FILES.get("company_logo"),
            email=company_email,
            phone=company_phone,
            address=company_address,
            country=company_country,
            business_reg_no=business_reg_no,
            business_start_date=business_start_date or None,

            contact_name=contact_name,
            contact_position=contact_position,
            contact_phone=contact_phone,
            contact_email=contact_email,

            tax_id=tax_id,
            credit_limit=credit_limit,
            payment_terms=payment_terms,

            currency="UGX",

            industry=industry,
            notes=notes,

            billing_cycle=billing_cycle,
            bill_to=bill_to,
            primary_admin=primary_admin,

            payment_method=None,
            payment_reference=None,

            created_by=request.user,
            is_active=True,
        )

        plan = _plan_from_pkg(selected_package)
        max_users = _max_users_for_plan(plan)

        start_date = timezone.now().date()
        end_date = (timezone.now() + timezone.timedelta(days=30)).date()

        dt_start = _safe_parse_iso_datetime(trial_starts_at)
        dt_end = _safe_parse_iso_datetime(trial_ends_at)
        if dt_start:
            start_date = dt_start.date()
        if dt_end:
            end_date = dt_end.date()

        Subscription.objects.create(
            company=company,
            plan=plan,
            status="TRIAL",
            start_date=start_date,
            end_date=end_date,
            max_users=max_users,
        )

        owner_user = User.objects.filter(email__iexact=company_email).first()
        if not owner_user:
            base = (company_email.split("@")[0] or "client")[:25]
            username = base
            i = 1
            while User.objects.filter(username__iexact=username).exists():
                i += 1
                username = f"{base}{i}"

            owner_user = User.objects.create_user(username=username, email=company_email, password=None)
            owner_user.set_unusable_password()
            owner_user.save()

        CompanyMember.objects.create(
            company=company,
            user=owner_user,
            role="OWNER",
            is_active=True,
        )

        invite = CompanyInvite.create(company, company_email, role="OWNER", hours=24, created_by=request.user)
        try:
            _send_invite_email(request, invite)
        except Exception:
            messages.warning(request, "Company created, but invite email failed. Check email settings.")

        messages.success(request, f"Free trial activated for '{company.name}'. Invite sent to {company_email}.")
        return redirect("tenancy:company_list")

    return render(request, "Clients_form.html")


@require_http_methods(["GET", "POST"])
def client_invite(request, token):
    """
    Invite link entry point.
    - DO NOT mark used yet.
    - Store token in session.
    - Redirect to OTP login (prefill email).
    """
    invite = CompanyInvite.objects.filter(token=token).select_related("company").first()
    if not invite or not invite.is_valid():
        messages.error(request, "Invite link invalid or expired.")
        return redirect("tenancy:client_otp_login")

    request.session["invite_token"] = invite.token

    url = reverse("tenancy:client_otp_login") + f"?email={invite.email}"
    return redirect(url)


@require_http_methods(["GET", "POST"])
def client_otp_login(request):
    """
    OTP request screen.
    - Allows login by email IF:
        a) they are already a member of any active company OR
        b) they have a valid invite_token session
    """
    if request.method == "POST":
        email = (request.POST.get("email") or "").strip().lower()
        if not email:
            messages.error(request, "Enter your email.")
            return render(request, "client_otp_login.html", {"email": email})

        invite_token = request.session.get("invite_token")
        invite = None

        if invite_token:
            invite = CompanyInvite.objects.filter(
                token=invite_token,
                email__iexact=email
            ).select_related("company").first()

            if not invite or not invite.is_valid():
                request.session.pop("invite_token", None)
                invite = None

        if not invite:
            user = User.objects.filter(email__iexact=email).first()
            if not user:
                messages.error(request, "No active account found for that email.")
                return render(request, "client_otp_login.html", {"email": email})

            is_member_anywhere = CompanyMember.objects.filter(
                user=user,
                is_active=True,
                company__is_active=True
            ).exists()

            if not is_member_anywhere:
                messages.error(request, "You are not assigned to any company yet.")
                return render(request, "client_otp_login.html", {"email": email})

        last = EmailOTP.objects.filter(
            email__iexact=email,
            purpose="LOGIN",
            is_used=False
        ).order_by("-created_at").first()

        now = timezone.now()

        if last and last.last_sent_at:
            seconds_since = (now - last.last_sent_at).total_seconds()
            if seconds_since < OTP_RESEND_COOLDOWN_SECONDS:
                wait = int(OTP_RESEND_COOLDOWN_SECONDS - seconds_since)
                messages.error(request, f"Please wait {wait}s then try again.")
                return render(request, "client_otp_login.html", {"email": email})

        code = EmailOTP.generate_code(6)
        user = User.objects.filter(email__iexact=email).first()

        otp = EmailOTP.objects.create(
            user=user,
            email=email,
            purpose="LOGIN",
            expires_at=now + timezone.timedelta(minutes=OTP_TTL_MINUTES),
            last_sent_at=now,
            send_count=1,
        )
        otp.set_code(code)
        otp.save()

        _send_otp_email(email, code)

        request.session["otp_login_id"] = otp.id
        request.session["otp_login_email"] = email

        messages.success(request, f"OTP sent to {email}.")
        return redirect("tenancy:client_otp_verify")

    email_prefill = (request.GET.get("email") or "").strip()
    return render(request, "client_otp_login.html", {"email": email_prefill})


@require_http_methods(["GET", "POST"])
def client_otp_verify(request):
    """
    OTP verify screen.
    After success:
      - if invite_token exists -> accept invite
      - else -> normal login for existing member
    """
    otp_id = request.session.get("otp_login_id")
    email = request.session.get("otp_login_email")

    if not otp_id or not email:
        messages.error(request, "Please enter your email to receive an OTP.")
        return redirect("tenancy:client_otp_login")

    otp = EmailOTP.objects.filter(id=otp_id, email__iexact=email, purpose="LOGIN").first()
    if not otp:
        messages.error(request, "OTP session not found. Please request a new code.")
        return redirect("tenancy:client_otp_login")

    if request.method == "POST":
        code = (request.POST.get("code") or "").strip()
        if not code:
            messages.error(request, "Enter the OTP code.")
            return render(request, "client_otp_verify.html", {"email": email})

        if not otp.can_attempt():
            messages.error(request, "OTP expired / used / too many attempts. Request a new OTP.")
            return redirect("tenancy:client_otp_login")

        otp.attempts += 1
        otp.save(update_fields=["attempts"])

        if not otp.check_code(code):
            remaining = max(0, otp.max_attempts - otp.attempts)
            messages.error(request, f"Invalid code. Attempts left: {remaining}.")
            return render(request, "client_otp_verify.html", {"email": email})

        otp.is_used = True
        otp.save(update_fields=["is_used"])

        invite_token = request.session.get("invite_token")
        if invite_token:
            invite = CompanyInvite.objects.filter(
                token=invite_token,
                email__iexact=email
            ).select_related("company").first()

            if invite and invite.is_valid():
                user = _get_or_create_user_for_email(email)

                CompanyMember.objects.get_or_create(
                    company=invite.company,
                    user=user,
                    defaults={"role": invite.role, "is_active": True},
                )

                invite.used_at = timezone.now()
                invite.save(update_fields=["used_at"])

                request.session["company_id"] = invite.company_id
                request.session["workspace_mode"] = "client"

                login(request, user, backend="django.contrib.auth.backends.ModelBackend")

                request.session.pop("otp_login_id", None)
                request.session.pop("otp_login_email", None)
                request.session.pop("invite_token", None)

                messages.success(request, "Login successful. Invite accepted.")
                return redirect("sowaf:home")

            request.session.pop("invite_token", None)

        user = otp.user or User.objects.filter(email__iexact=email).first()
        if not user:
            messages.error(request, "Account not found. Please contact support.")
            return redirect("tenancy:client_otp_login")

        is_member_anywhere = CompanyMember.objects.filter(
            user=user, is_active=True, company__is_active=True
        ).exists()
        if not is_member_anywhere:
            messages.error(request, "You are not assigned to any company yet.")
            return redirect("tenancy:client_otp_login")

        login(request, user, backend="django.contrib.auth.backends.ModelBackend")
        _set_default_company_in_session(request, user)

        request.session.pop("otp_login_id", None)
        request.session.pop("otp_login_email", None)

        messages.success(request, "Login successful.")
        return redirect("sowaf:home")

    return render(request, "client_otp_verify.html", {"email": email})


@login_required
@module_required("settings")
def client_settings_users(request):
    """
    Owner/Manager sees company users, invite new, update roles, deactivate.
    """
    company = getattr(request, "company", None)
    if not company:
        messages.error(request, "Select a company first.")
        return redirect("sowaf:home")

    membership = get_membership(request)
    if not membership or membership.role not in ("OWNER", "MANAGER"):
        messages.error(request, "Not allowed.")
        return redirect("sowaf:home")

    members = (
        CompanyMember.objects
        .filter(company=company)
        .select_related("user")
        .order_by("role", "user__username")
    )
    invites = CompanyInvite.objects.filter(company=company).order_by("-created_at")[:20]

    return render(request, "client_settings_users.html", {
        "company": company,
        "members": members,
        "invites": invites,
        "roles": CompanyMember.ROLE_CHOICES,
    })


@login_required
@module_required("settings")
@require_http_methods(["POST"])
def client_invite_user(request):
    company = getattr(request, "company", None)
    if not company:
        messages.error(request, "Select a company first.")
        return redirect("sowaf:home")

    membership = get_membership(request)
    if not membership or membership.role not in ("OWNER", "MANAGER"):
        messages.error(request, "Not allowed.")
        return redirect("sowaf:home")

    email = (request.POST.get("email") or "").strip().lower()
    role = (request.POST.get("role") or "VIEWER").strip().upper()

    if not email:
        messages.error(request, "Email is required.")
        return redirect("tenancy:client_settings_users")

    subscription = getattr(company, "subscription", None)
    if subscription:
        active_count = CompanyMember.objects.filter(company=company, is_active=True).count()
        if active_count >= subscription.max_users:
            messages.error(request, f"User limit reached ({subscription.max_users}). Upgrade plan to add more users.")
            return redirect("tenancy:client_settings_users")

    invite = CompanyInvite.create(company, email, role=role, hours=24, created_by=request.user)
    try:
        _send_invite_email(request, invite)
        messages.success(request, f"Invite sent to {email}.")
    except Exception:
        messages.error(request, "Invite created but email failed. Check email settings.")

    return redirect("tenancy:client_settings_users")


@login_required
@module_required("settings")
@require_http_methods(["POST"])
def client_update_member_role(request, member_id):
    company = getattr(request, "company", None)
    membership = get_membership(request)
    if not company or not membership or membership.role not in ("OWNER", "MANAGER"):
        messages.error(request, "Not allowed.")
        return redirect("sowaf:home")

    member = get_object_or_404(CompanyMember, id=member_id, company=company)
    new_role = (request.POST.get("role") or member.role).strip().upper()

    if member.user_id == request.user.id and member.role == "OWNER" and new_role != "OWNER":
        messages.error(request, "Owner cannot remove their own OWNER role.")
        return redirect("tenancy:client_settings_users")

    member.role = new_role
    member.save(update_fields=["role"])
    messages.success(request, "Role updated.")
    return redirect("tenancy:client_settings_users")


@login_required
@module_required("settings")
@require_http_methods(["POST"])
def client_deactivate_member(request, member_id):
    company = getattr(request, "company", None)
    membership = get_membership(request)
    if not company or not membership or membership.role not in ("OWNER", "MANAGER"):
        messages.error(request, "Not allowed.")
        return redirect("sowaf:home")

    member = get_object_or_404(CompanyMember, id=member_id, company=company)

    if member.role == "OWNER":
        owners = CompanyMember.objects.filter(company=company, role="OWNER", is_active=True).count()
        if owners <= 1:
            messages.error(request, "You cannot deactivate the last OWNER.")
            return redirect("tenancy:client_settings_users")

    member.is_active = False
    member.save(update_fields=["is_active"])
    messages.success(request, "User deactivated.")
    return redirect("tenancy:client_settings_users")