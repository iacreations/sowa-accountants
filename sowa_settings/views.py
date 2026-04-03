from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from decimal import Decimal

from .models import CompanySettings, Currency
from .services import refresh_company_rates, convert_company_gl
from tenancy.models import CompanyMember, CompanyInvite


@login_required
def company_settings_view(request):
    """
    Settings page with left sidebar:
      - Company info
      - Currencies
      - Team & Users
    `tab` GET param selects which one is active.
    """
    tab = request.GET.get("tab", "company")
    company = getattr(request, "company", None)

    # ── per-company settings row ──────────────────────────────────
    if company:
        settings_obj, _ = CompanySettings.objects.get_or_create(
            company=company,
            defaults={"created_by": request.user},
        )
    else:
        settings_obj, _ = CompanySettings.objects.get_or_create(
            pk=1,
            defaults={"created_by": request.user},
        )

    # Ensure home currency row exists when currency is set
    if company and company.currency:
        Currency.objects.get_or_create(
            company=company,
            code=company.currency,
            defaults={
                "name": company.currency,
                "is_home": True,
                "rate_to_home": 1,
            },
        )

    # Filter currencies by company
    if company:
        currencies = Currency.objects.filter(company=company).order_by("-is_home", "code")
    else:
        currencies = Currency.objects.filter(company__isnull=True).order_by("-is_home", "code")

    if request.method == "POST":
        form_type = request.POST.get("form_type")

        # ----- COMPANY INFO FORM -------------------------------------------
        if form_type == "company":
            settings_obj.company_name = request.POST.get("company_name", "").strip()
            settings_obj.address = request.POST.get("address", "").strip()
            settings_obj.email = request.POST.get("email", "").strip()
            settings_obj.phone = request.POST.get("phone", "").strip()

            reporting_currency = request.POST.get("reporting_currency", "").strip().upper()
            if company and not company.currency_locked and reporting_currency:
                old_currency = company.currency  # e.g. "UGX"
                # Lock the currency on the Company record
                company.original_currency = old_currency
                company.currency = reporting_currency
                company.currency_locked = True
                company.save(update_fields=[
                    "currency", "currency_locked", "original_currency",
                ])
                # Mirror to CompanySettings
                settings_obj.reporting_currency = reporting_currency
                settings_obj.currency_locked = True
                # Create home currency row
                Currency.objects.get_or_create(
                    company=company,
                    code=reporting_currency,
                    defaults={
                        "name": reporting_currency,
                        "is_home": True,
                        "rate_to_home": 1,
                    },
                )
                # Trigger initial rate fetch
                refresh_company_rates(company)
                # Convert existing GL amounts from old currency to new
                converted = 0
                if old_currency != reporting_currency:
                    converted = convert_company_gl(company, old_currency)
                msg = f"Reporting currency locked to {reporting_currency}. "
                if converted:
                    msg += f"{converted} journal entries converted from {old_currency}."
                else:
                    msg += "Exchange rates have been fetched."
                messages.success(request, msg)
            elif not company and not settings_obj.currency_locked and reporting_currency:
                settings_obj.reporting_currency = reporting_currency
                settings_obj.currency_locked = True

            settings_obj.save()
            if not (company and not company.currency_locked):
                messages.success(request, "Company information saved.")
            return redirect(f"{request.path}?tab=company")

        # ----- ADD / UPDATE CURRENCY RATES --------------------------------
        elif form_type == "currency_add":
            code = request.POST.get("code", "").upper().strip()
            name = request.POST.get("name", "").strip()
            rate_str = request.POST.get("rate_to_home", "").strip()
            if code and name and rate_str:
                try:
                    rate_val = Decimal(rate_str)
                except Exception:
                    messages.error(request, "Invalid rate value.")
                    return redirect(f"{request.path}?tab=currencies")

                cur, _ = Currency.objects.get_or_create(
                    company=company, code=code,
                    defaults={"name": name},
                )
                cur.name = name
                if not cur.is_home:
                    cur.rate_to_home = rate_val
                cur.is_active = True
                cur.save()
                messages.success(request, f"Currency {code} saved.")
            else:
                messages.error(request, "Currency code, name and rate are required.")
            return redirect(f"{request.path}?tab=currencies")

        elif form_type == "currency_update":
            for cur in currencies:
                field_name = f"rate_{cur.id}"
                if field_name in request.POST:
                    value = request.POST.get(field_name, "").strip()
                    if not cur.is_home and value:
                        try:
                            cur.rate_to_home = Decimal(value)
                            cur.save()
                        except Exception:
                            messages.error(request, f"Invalid rate for {cur.code}.")
            messages.success(request, "Currency rates updated.")
            return redirect(f"{request.path}?tab=currencies")

        # ----- REFRESH RATES FROM API ------------------------------------
        elif form_type == "currency_refresh":
            if company and company.currency_locked:
                count = refresh_company_rates(company)
                if count:
                    messages.success(request, f"Refreshed {count} exchange rates.")
                else:
                    messages.error(request, "Could not fetch rates. Try again later.")
            else:
                messages.error(request, "Set and lock a home currency first.")
            return redirect(f"{request.path}?tab=currencies")

    return render(
        request,
        "settings.html",
        {
            "tab": tab,
            "settings_obj": settings_obj,
            "currencies": currencies,
            "company": company,
            "currency_locked": company.currency_locked if company else settings_obj.currency_locked,
            "home_currency": company.currency if company else settings_obj.reporting_currency,
            "is_owner_or_manager": _is_owner_or_manager(request),
            "members": _get_team_members(request) if tab == "team" else [],
            "invites": _get_team_invites(request) if tab == "team" else [],
            "roles": CompanyMember.ROLE_CHOICES if tab == "team" else [],
        },
    )


def _is_owner_or_manager(request):
    company = getattr(request, "company", None)
    if not company or not request.user.is_authenticated:
        return False
    membership = CompanyMember.objects.filter(
        company=company, user=request.user, is_active=True
    ).first()
    return membership and membership.role in ("OWNER", "MANAGER")


def _get_team_members(request):
    company = getattr(request, "company", None)
    if not company:
        return []
    return (
        CompanyMember.objects
        .filter(company=company)
        .select_related("user")
        .order_by("role", "user__username")
    )


def _get_team_invites(request):
    company = getattr(request, "company", None)
    if not company:
        return []
    return CompanyInvite.objects.filter(company=company).order_by("-created_at")[:20]