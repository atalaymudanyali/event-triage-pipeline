from prometheus_client import Counter, Histogram

from triage_pipeline.metrics import (
    DB_SAVE_DURATION,
    DLT_EVENTS,
    EVENTS_PROCESSED,
    TRIAGE_DURATION,
    TRIAGE_ERRORS,
    TRIAGE_RETRIES,
)


class TestMetricTypes:
    def test_counters(self):
        assert isinstance(EVENTS_PROCESSED, Counter)
        assert isinstance(TRIAGE_ERRORS, Counter)
        assert isinstance(TRIAGE_RETRIES, Counter)
        assert isinstance(DLT_EVENTS, Counter)

    def test_histograms(self):
        assert isinstance(TRIAGE_DURATION, Histogram)
        assert isinstance(DB_SAVE_DURATION, Histogram)


class TestMetricLabels:
    def test_events_processed_labels(self):
        assert EVENTS_PROCESSED._labelnames == ("category", "urgency")

    def test_triage_duration_labels(self):
        assert TRIAGE_DURATION._labelnames == ("step",)

    def test_triage_errors_labels(self):
        assert TRIAGE_ERRORS._labelnames == ("error_type",)


class TestHistogramBuckets:
    def test_triage_duration_buckets(self):
        assert TRIAGE_DURATION._kwargs["buckets"] == (0.5, 1, 2, 5, 10, 20, 30, 60)

    def test_db_save_duration_buckets(self):
        assert DB_SAVE_DURATION._kwargs["buckets"] == (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1)
