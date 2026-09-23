"""
business_time.py — business-hours/business-day math for the reply-time
metrics (DMs within 4 business hours, comments within 1 business day).

Evaluated in each studio's own local timezone (from its state, via
studios.json), not a single company-wide timezone — a coach in Utah
replying "within 4 business hours" should be judged by Utah business
hours, not Eastern.

Simplifications (disclosed, not hidden):
  - State -> single IANA timezone. A handful of states technically span two
    zones (TX, TN); every studio currently in this registry happens to sit
    in that state's majority zone, so this is exact for now, not just close.
  - Business hours are a fixed Mon-Fri 9am-6pm window. No holiday calendar.
"""
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

STATE_TIMEZONES = {
    "AL": "America/Chicago",
    "FL": "America/New_York",
    "GA": "America/New_York",
    "MA": "America/New_York",
    "NC": "America/New_York",
    "NJ": "America/New_York",
    "NY": "America/New_York",
    "OK": "America/Chicago",
    "TN": "America/Chicago",
    "TX": "America/Chicago",
    "UT": "America/Denver",
    "VA": "America/New_York",
}
DEFAULT_TIMEZONE = "America/New_York"

BUSINESS_START_HOUR = 9
BUSINESS_END_HOUR = 18  # 6pm


def studio_timezone(state: str | None) -> ZoneInfo:
    return ZoneInfo(STATE_TIMEZONES.get(state, DEFAULT_TIMEZONE))


def _is_business_day(d) -> bool:
    return d.weekday() < 5  # Mon-Fri


def business_hours_between(start: datetime, end: datetime) -> float:
    """Business hours elapsed between two tz-aware datetimes, counting only
    Mon-Fri 9am-6pm in whatever timezone they're already in. Returns 0 if
    end <= start."""
    if end <= start:
        return 0.0

    total = timedelta()
    cur = start
    while cur < end:
        day_start = cur.replace(hour=BUSINESS_START_HOUR, minute=0, second=0, microsecond=0)
        day_end = cur.replace(hour=BUSINESS_END_HOUR, minute=0, second=0, microsecond=0)

        if not _is_business_day(cur) or cur >= day_end:
            # Past today's business window (or a weekend) — jump to tomorrow 9am.
            cur = (cur + timedelta(days=1)).replace(
                hour=BUSINESS_START_HOUR, minute=0, second=0, microsecond=0)
            continue
        if cur < day_start:
            cur = day_start
            continue

        slice_end = min(end, day_end)
        total += slice_end - cur
        cur = slice_end

    return total.total_seconds() / 3600


def next_business_day_end(moment: datetime) -> datetime:
    """End-of-day (23:59:59) of the next business day strictly after
    moment's calendar date. A comment posted Friday or over the weekend
    both land on the same deadline — the next Monday — since neither
    Saturday nor Sunday is itself a business day to count from."""
    d = moment.date()
    while True:
        d = d + timedelta(days=1)
        if d.weekday() < 5:
            return datetime.combine(d, time(23, 59, 59), tzinfo=moment.tzinfo)
