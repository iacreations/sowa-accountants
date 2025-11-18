from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib import messages

from .models import CompanySettings, Currency


@login_required
def company_settings_view(request):
    """
    Settings page with left sidebar:
      - Company info
      - Currencies
    `tab` GET param selects which one is active.
    """
    tab = request.GET.get("tab", "company")  # 'company' or 'currencies'

    settings_obj, created = CompanySettings.objects.get_or_create(
        pk=1,
        defaults={"created_by": request.user},
    )

    # Ensure home currency row exists if reporting_currency is set
    if settings_obj.reporting_currency:
        Currency.objects.get_or_create(
            code=settings_obj.reporting_currency,
            defaults={
                "name": settings_obj.reporting_currency,
                "is_home": True,
                "rate_to_home": 1,
            },
        )

    currencies = Currency.objects.all().order_by("-is_home", "code")

    if request.method == "POST":
        form_type = request.POST.get("form_type")

        # ----- COMPANY INFO FORM -------------------------------------------
        if form_type == "company":
            settings_obj.company_name = request.POST.get("company_name", "").strip()
            settings_obj.address = request.POST.get("address", "").strip()
            settings_obj.email = request.POST.get("email", "").strip()
            settings_obj.phone = request.POST.get("phone", "").strip()

            reporting_currency = request.POST.get("reporting_currency", "").strip().upper()
            if not settings_obj.currency_locked:
                if reporting_currency:
                    settings_obj.reporting_currency = reporting_currency
                    settings_obj.currency_locked = True

            settings_obj.save()
            messages.success(request, "Company information saved.")
            return redirect(f"{request.path}?tab=company")

        # ----- ADD / UPDATE CURRENCY RATES --------------------------------
        elif form_type == "currency_add":
            code = request.POST.get("code", "").upper().strip()
            name = request.POST.get("name", "").strip()
            rate_str = request.POST.get("rate_to_home", "").strip()
            if code and name and rate_str:
                from decimal import Decimal
                try:
                    rate_val = Decimal(rate_str)
                except Exception:
                    messages.error(request, "Invalid rate value.")
                    return redirect(f"{request.path}?tab=currencies")

                cur, _ = Currency.objects.get_or_create(code=code, defaults={"name": name})
                cur.name = name
                # never allow changing home currency rate from 1
                if not cur.is_home:
                    cur.rate_to_home = rate_val
                cur.is_active = True
                cur.save()
                messages.success(request, f"Currency {code} saved.")
            else:
                messages.error(request, "Currency code, name and rate are required.")

            return redirect(f"{request.path}?tab=currencies")

        elif form_type == "currency_update":
            from decimal import Decimal
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

    return render(
        request,
        "settings.html",
        {
            "tab": tab,
            "settings_obj": settings_obj,
            "currencies": currencies,
        },
    )