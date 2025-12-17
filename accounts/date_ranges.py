# accounts/date_ranges.py
from datetime import timedelta
from django.utils import timezone

RANGE_LABELS = {
    "last_30_days": "Last 30 days",
    "this_month": "This month",
    "this_month_to_date": "This month to date",
    "this_fiscal_quarter": "This fiscal quarter",
    "this_fiscal_quarter_to_date": "This fiscal quarter to date",
    "this_financial_year": "This financial year",
    "this_financial_year_to_date": "This financial year to date",
    "last_month": "Last month",
    "last_fiscal_quarter": "Last fiscal quarter",
    "last_financial_year": "Last financial year",
}

RANGE_OPTIONS = [
    ("last_30_days", "Last 30 days"),
    ("this_month", "This month"),
    ("this_month_to_date", "This month to date"),
    ("this_fiscal_quarter", "This fiscal quarter"),
    ("this_fiscal_quarter_to_date", "This fiscal quarter to date"),
    ("this_financial_year", "This financial year"),
    ("this_financial_year_to_date", "This financial year to date"),
    ("last_month", "Last month"),
    ("last_fiscal_quarter", "Last fiscal quarter"),
    ("last_financial_year", "Last financial year"),
]


def _end_of_month(d):
    # first day next month - 1 day
    if d.month == 12:
        first_next = d.replace(year=d.year + 1, month=1, day=1)
    else:
        first_next = d.replace(month=d.month + 1, day=1)
    return first_next - timedelta(days=1)


def _quarter_start_end(d):
    q = (d.month - 1) // 3  # 0..3
    start_month = q * 3 + 1
    start = d.replace(month=start_month, day=1)

    end_month = start_month + 2
    end_base = d.replace(month=end_month, day=1)
    end = _end_of_month(end_base)
    return start, end


def resolve_date_range(key: str):
    """
    Returns (start_date, end_date) inclusive, based on key.
    Assumes fiscal quarter == calendar quarter,
    financial year == calendar year (Jan-Dec).
    """
    today = timezone.localdate()

    if key == "last_30_days":
        return today - timedelta(days=29), today

    if key == "this_month":
        start = today.replace(day=1)
        end = _end_of_month(today)
        return start, end

    if key == "this_month_to_date":
        start = today.replace(day=1)
        end = today
        return start, end

    if key == "this_fiscal_quarter":
        start, end = _quarter_start_end(today)
        return start, end

    if key == "this_fiscal_quarter_to_date":
        start, _end = _quarter_start_end(today)
        return start, today

    if key == "this_financial_year":
        start = today.replace(month=1, day=1)
        end = today.replace(month=12, day=31)
        return start, end

    if key == "this_financial_year_to_date":
        start = today.replace(month=1, day=1)
        end = today
        return start, end

    if key == "last_month":
        first_this = today.replace(day=1)
        last_month_end = first_this - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        return last_month_start, last_month_end

    if key == "last_fiscal_quarter":
        this_q_start, _this_q_end = _quarter_start_end(today)
        prev_q_end = this_q_start - timedelta(days=1)
        prev_q_start, prev_q_end2 = _quarter_start_end(prev_q_end)
        return prev_q_start, prev_q_end2

    if key == "last_financial_year":
        start = today.replace(year=today.year - 1, month=1, day=1)
        end = today.replace(year=today.year - 1, month=12, day=31)
        return start, end

    # default fallback
    start = today - timedelta(days=29)
    end = today
    return start, end
