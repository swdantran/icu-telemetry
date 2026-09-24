
import pytest

from services.alerts.engine import (
    check_thresholds,
    check_trend,
    deduped,
    windows,
    last_fired,
    COOLDOWN,
)


@pytest.fixture(autouse=True)
def reset_state():
    """Prevent tests from affecting each other."""
    windows.clear()
    last_fired.clear()
    yield
    windows.clear()
    last_fired.clear()


def reading(hr=80, spo2=98):
    return {
        "patient_id": "p1",
        "hr": hr,
        "spo2": spo2,
        "bp_sys": 120,
        "bp_dia": 80,
        "resp_rate": 16,
    }


def test_normal_vitals():
    assert check_thresholds(reading()) == []


def test_low_spo2_warning():
    hits = check_thresholds(reading(spo2=92))

    assert len(hits) == 1
    assert hits[0][0] == "warning"


def test_low_spo2_critical():
    hits = check_thresholds(reading(spo2=85))

    assert len(hits) == 1
    assert hits[0][0] == "critical"


def test_multiple_critical_conditions():
    e = reading(hr=145, spo2=85)
    e["bp_sys"] = 80

    hits = check_thresholds(e)

    assert len(hits) == 3
    assert all(hit[0] == "critical" for hit in hits)


def test_threshold_boundary():
    # Threshold comparisons are strict (< and >).
    assert check_thresholds(reading(spo2=93)) == []
    assert check_thresholds(reading(hr=115)) == []


def test_no_trend_without_history():
    assert check_trend(reading()) == []


def test_trend_detection():
    # Build a stable 60-reading baseline.
    for _ in range(60):
        check_trend(reading())

    hits = check_trend(reading(hr=130, spo2=85))

    assert len(hits) == 2
    assert all(hit[0] == "warning" for hit in hits)


def test_alert_cooldown():
    hits = check_thresholds(reading(spo2=85))

    first = deduped("p1", hits, 100)
    second = deduped("p1", hits, 110)
    third = deduped("p1", hits, 100 + COOLDOWN)

    assert len(first) == 1
    assert len(second) == 0
    assert len(third) == 1


def test_cooldown_independent_per_patient():
    hits = check_thresholds(reading(spo2=85))

    p1 = deduped("p1", hits, 100)
    p2 = deduped("p2", hits, 100)

    assert len(p1) == 1
    assert len(p2) == 1