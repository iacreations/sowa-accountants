"""
Exchange-rate helpers.
Uses the free Open ExchangeRate-API (open.er-api.com) — no API key needed,
~1 500 requests / month, 150+ currencies (UGX, KES, TZS, RWF, ZAR, …).
"""
import logging
from decimal import Decimal, ROUND_HALF_UP

import requests
from django.utils import timezone

from .models import Currency

logger = logging.getLogger(__name__)

API_URL = "https://open.er-api.com/v6/latest/{base}"
REQUEST_TIMEOUT = 15  # seconds


# ── Common currency catalogue (code → full name) ──────────────────
CURRENCY_NAMES = {
    "UGX": "Ugandan Shilling",
    "USD": "US Dollar",
    "EUR": "Euro",
    "GBP": "British Pound",
    "KES": "Kenyan Shilling",
    "TZS": "Tanzanian Shilling",
    "RWF": "Rwandan Franc",
    "ZAR": "South African Rand",
    "NGN": "Nigerian Naira",
    "GHS": "Ghanaian Cedi",
    "INR": "Indian Rupee",
    "JPY": "Japanese Yen",
    "CNY": "Chinese Yuan",
    "CAD": "Canadian Dollar",
    "AUD": "Australian Dollar",
    "CHF": "Swiss Franc",
    "AED": "UAE Dirham",
    "SAR": "Saudi Riyal",
    "BRL": "Brazilian Real",
    "MXN": "Mexican Peso",
}


def fetch_rates(base_currency: str) -> dict | None:
    """
    Call ExchangeRate-API and return the 'rates' dict keyed by currency code,
    or *None* on failure.
    """
    url = API_URL.format(base=base_currency.upper())
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("result") == "success":
            return data.get("rates", {})
        logger.warning("ExchangeRate-API error: %s", data)
    except requests.RequestException as exc:
        logger.error("ExchangeRate-API request failed: %s", exc)
    return None


def refresh_company_rates(company) -> int:
    """
    Fetch live rates for *company*'s home currency and upsert every
    Currency row that belongs to this company.
    Returns the number of currencies updated (0 on failure).
    """
    home_code = company.currency
    if not home_code:
        return 0

    rates = fetch_rates(home_code)
    if rates is None:
        return 0

    updated = 0
    now = timezone.now()

    for code, rate_value in rates.items():
        if code == home_code:
            continue  # skip home → home (always 1)

        rate_decimal = Decimal(str(rate_value)).quantize(
            Decimal("0.000001"), rounding=ROUND_HALF_UP
        )
        # rate_to_home: how many units of *home* currency 1 unit of this
        # foreign currency buys.  The API returns the inverse (home→foreign),
        # so we invert it: rate_to_home = 1 / api_rate.
        # Example: home=UGX, API says USD=0.000268 (i.e. 1 UGX=0.000268 USD).
        # Invert → 1 USD = 3731.34 UGX, which is what rate_to_home stores.
        if rate_decimal == 0:
            continue
        inverted = (Decimal("1") / rate_decimal).quantize(
            Decimal("0.000001"), rounding=ROUND_HALF_UP
        )

        name = CURRENCY_NAMES.get(code, code)
        cur, created = Currency.objects.update_or_create(
            company=company,
            code=code,
            defaults={
                "name": name,
                "rate_to_home": inverted,
                "is_home": False,
                "is_active": True,
            },
        )
        updated += 1

    # Ensure home currency row exists
    Currency.objects.update_or_create(
        company=company,
        code=home_code,
        defaults={
            "name": CURRENCY_NAMES.get(home_code, home_code),
            "is_home": True,
            "rate_to_home": Decimal("1"),
            "is_active": True,
        },
    )

    logger.info("Refreshed %d rates for %s (home=%s)", updated, company, home_code)
    return updated


def convert_to_home(amount, from_currency_code: str, company) -> Decimal:
    """
    Convert *amount* in *from_currency_code* to the company's home currency.
    """
    if from_currency_code == company.currency:
        return Decimal(str(amount))

    try:
        cur = Currency.objects.get(company=company, code=from_currency_code)
    except Currency.DoesNotExist:
        raise ValueError(
            f"No exchange rate for {from_currency_code} in {company}"
        )

    return (Decimal(str(amount)) * cur.rate_to_home).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def convert_from_home(amount, to_currency_code: str, company) -> Decimal:
    """
    Convert *amount* from the company's home currency to *to_currency_code*.
    """
    if to_currency_code == company.currency:
        return Decimal(str(amount))

    try:
        cur = Currency.objects.get(company=company, code=to_currency_code)
    except Currency.DoesNotExist:
        raise ValueError(
            f"No exchange rate for {to_currency_code} in {company}"
        )

    if cur.rate_to_home == 0:
        raise ValueError(f"Zero rate for {to_currency_code}")

    return (Decimal(str(amount)) / cur.rate_to_home).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def convert_company_gl(company, from_currency_code: str) -> int:
    """
    One-time bulk conversion of all JournalLine amounts for *company*
    from *from_currency_code* to the company's current home currency.

    Returns the number of journal lines converted.
    """
    if from_currency_code == company.currency:
        logger.info("No conversion needed — same currency.")
        return 0

    try:
        cur = Currency.objects.get(company=company, code=from_currency_code)
        fx_rate = cur.rate_to_home  # e.g. 0.000267 for UGX when home=USD
    except Currency.DoesNotExist:
        logger.error("No exchange rate for %s under %s", from_currency_code, company)
        return 0

    if fx_rate == 0 or fx_rate == Decimal("1"):
        logger.warning("fx_rate is %s — skipping conversion.", fx_rate)
        return 0

    from accounts.models import JournalLine, JournalEntry

    lines = JournalLine.objects.filter(entry__company=company)
    count = 0
    for jl in lines.iterator():
        jl.debit = (jl.debit * fx_rate).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        jl.credit = (jl.credit * fx_rate).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        jl.save(update_fields=["debit", "credit"])
        count += 1

    logger.info(
        "Converted %d journal lines for %s: %s → %s (rate=%s)",
        count, company, from_currency_code, company.currency, fx_rate,
    )
    return count
