import datetime

import pytest

from d1clone.quartz import QuartzSchedule, UnsupportedSchedule

UTC = datetime.timezone.utc


def t(*args):
    return datetime.datetime(*args, tzinfo=UTC)


def test_every_three_minutes_at_ten_seconds():
    s = QuartzSchedule("10 0/3 * * * ? *")                 # the common staging schedule
    assert s.next_after(t(2026, 1, 5, 12, 0, 0)) == t(2026, 1, 5, 12, 0, 10)
    assert s.next_after(t(2026, 1, 5, 12, 0, 10)) == t(2026, 1, 5, 12, 3, 10)
    assert s.fires_after(t(2026, 1, 5, 12, 59, 59), 2) == [t(2026, 1, 5, 13, 0, 10), t(2026, 1, 5, 13, 3, 10)]


def test_daily_schedule():
    s = QuartzSchedule("0 9 9 * * ? *")                    # daily at 09:09:00
    assert s.next_after(t(2026, 1, 5, 9, 9, 0)) == t(2026, 1, 6, 9, 9, 0)
    assert s.fires_between(t(2026, 1, 5, 0, 0, 0), t(2026, 1, 8, 0, 0, 0)) == 3


def test_six_fields_and_star_step():
    s = QuartzSchedule("0 */15 * * * ?")
    assert s.expression.endswith(" *")
    assert s.fires_between(t(2026, 1, 5, 12, 0, 0), t(2026, 1, 5, 13, 0, 0)) == 4


def test_day_of_week_uses_quartz_numbering_and_names():
    # Quartz: 1 = Sunday. 2026-01-04 is a Sunday.
    assert QuartzSchedule("0 0 0 ? * 1 *").next_after(t(2026, 1, 1)) == t(2026, 1, 4)
    assert QuartzSchedule("0 0 0 ? * SUN *").next_after(t(2026, 1, 1)) == t(2026, 1, 4)
    assert QuartzSchedule("0 0 0 ? * MON-FRI *").next_after(t(2026, 1, 3, 1)) == t(2026, 1, 5)


def test_lists_ranges_months_and_years():
    s = QuartzSchedule("30 15 6,18 1 JAN-MAR ? 2027")
    assert s.next_after(t(2026, 6, 1)) == t(2027, 1, 1, 6, 15, 30)
    assert s.next_after(t(2027, 1, 1, 6, 15, 30)) == t(2027, 1, 1, 18, 15, 30)
    assert s.next_after(t(2027, 1, 1, 18, 15, 30)) == t(2027, 2, 1, 6, 15, 30)


@pytest.mark.parametrize("expr", [
    "0 0 0 L * ? *",          # last day of month
    "0 0 0 ? * 6#3 *",        # third Friday
    "0 0 0 15W * ? *",        # nearest weekday
    "0 0 0 ? * ? *",          # both day fields '?'
    "0 0 0 1 * 2 *",          # both day fields restricted
    "0 0 0 * *",              # too few fields
    "0 0 0 31 2 ? *",         # never fires
])
def test_unsupported_expressions(expr):
    with pytest.raises(UnsupportedSchedule):
        QuartzSchedule(expr).next_after(t(2026, 1, 1))


def test_from_node_uses_the_registry_attribute_order():
    from types import SimpleNamespace
    node = SimpleNamespace(synchronization=SimpleNamespace(schedule=SimpleNamespace(
        sec="10", min="0/3", hour="*", mday="*", mon="*", wday="?", year="*")))
    assert QuartzSchedule.from_node(node).expression == "10 0/3 * * * ? *"
