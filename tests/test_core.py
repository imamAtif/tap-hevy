"""Tests for tap-hevy."""

from __future__ import annotations

import datetime
import json
from unittest.mock import patch

import pytest
import requests

from tap_hevy.client import HevyPageNumberPaginator
from tap_hevy.streams import (
    BodyMeasurementsStream,
    ExerciseHistoryStream,
    ExerciseTemplatesStream,
    RoutineFoldersStream,
    RoutinesStream,
    UserInfoStream,
    WorkoutEventsStream,
    WorkoutsStream,
)
from tap_hevy.tap import TapHevy

SAMPLE_CONFIG = {
    "api_key": "00000000-0000-0000-0000-000000000000",
    "start_date": "2024-01-01T00:00:00Z",
}

# Standard SDK tap tests - disabled for offline CI (requires live API mock).
# To enable, configure requests_mock via conftest.
# TestTapHevy = get_tap_test_class(
#     tap_class=TapHevy,
#     config=SAMPLE_CONFIG,
# )


# Helper fixtures
SAMPLE_WORKOUT = {
    "id": "b459cba5-cd6d-463c-abd6-54f8eafcadcb",
    "title": "Morning Workout",
    "description": "Pushed myself",
    "routine_id": "routine-123",
    "start_time": "2024-01-01T12:00:00Z",
    "end_time": "2024-01-01T13:00:00Z",
    "updated_at": "2024-01-01T13:00:00Z",
    "created_at": "2024-01-01T12:00:00Z",
    "exercises": [
        {
            "index": 0,
            "title": "Bench Press (Barbell)",
            "notes": "Felt great",
            "exercise_template_id": "05293BCA",
            "supersets_id": None,
            "sets": [
                {
                    "index": 0,
                    "type": "normal",
                    "weight_kg": 100,
                    "reps": 10,
                    "distance_meters": None,
                    "duration_seconds": None,
                    "rpe": 9.5,
                    "custom_metric": None,
                }
            ],
        }
    ],
}

SAMPLE_USER = {
    "id": "9c465af3-de7d-42bc-9c7c-f0170396358b",
    "name": "John doe",
    "url": "https://hevy.com/user/jhon",
}

SAMPLE_ROUTINE = {
    "id": "routine-123",
    "title": "Upper Body",
    "folder_id": 42,
    "updated_at": "2021-09-14T12:00:00Z",
    "created_at": "2021-09-14T12:00:00Z",
    "exercises": [],
}

SAMPLE_EXERCISE_TEMPLATE = {
    "id": "D04AC939",
    "title": "Bench Press (Barbell)",
    "type": "weight_reps",
    "primary_muscle_group": "chest",
    "secondary_muscle_groups": ["triceps"],
    "equipment_category": "barbell",
    "is_custom": False,
}

SAMPLE_ROUTINE_FOLDER = {
    "id": 42,
    "index": 1,
    "title": "Push Pull",
    "updated_at": "2021-09-14T12:00:00Z",
    "created_at": "2021-09-14T12:00:00Z",
}

SAMPLE_BODY = {
    "date": "2024-08-14",
    "weight_kg": 80.5,
    "lean_mass_kg": 65,
    "fat_percent": 18.5,
    "neck_cm": 38,
    "shoulder_cm": 115,
    "chest_cm": 95,
    "left_bicep_cm": 35,
    "right_bicep_cm": 35.5,
    "left_forearm_cm": 28,
    "right_forearm_cm": 28.5,
    "abdomen": 85,
    "waist": 80,
    "hips": 95,
    "left_thigh": 55,
    "right_thigh": 55.5,
    "left_calf": 37,
    "right_calf": 37.5,
}

SAMPLE_EXERCISE_HISTORY = {
    "exercise_history": [
        {
            "workout_id": "b459cba5-cd6d-463c-abd6-54f8eafcadcb",
            "workout_title": "Morning Workout",
            "workout_start_time": "2024-01-01T12:00:00Z",
            "workout_end_time": "2024-01-01T13:00:00Z",
            "exercise_template_id": "D04AC939",
            "weight_kg": 100,
            "reps": 10,
            "distance_meters": None,
            "duration_seconds": None,
            "rpe": 8.5,
            "custom_metric": None,
            "set_type": "normal",
        }
    ]
}

SAMPLE_WORKOUT_EVENTS_PAGE1 = {
    "page": 1,
    "page_count": 2,
    "events": [
        {"type": "updated", "workout": SAMPLE_WORKOUT},
        {"type": "deleted", "id": "deleted-id-123", "deleted_at": "2024-01-02T12:00:00Z"},
    ],
}
SAMPLE_WORKOUT_EVENTS_PAGE2 = {
    "page": 2,
    "page_count": 2,
    "events": [
        {
            "type": "updated",
            "workout": {**SAMPLE_WORKOUT, "id": "second-id", "updated_at": "2024-01-03T12:00:00Z"},
        },
    ],
}


def _make_response(
    json_data: dict,
    status_code: int = 200,
    headers: dict | None = None,
    url: str = "https://api.hevyapp.com/v1/workouts",
):
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = json.dumps(json_data).encode()
    resp.headers.update(headers or {})
    resp.url = url
    resp.reason = "OK" if status_code == 200 else "Error"
    return resp


class TestAuthentication:
    def test_api_key_header(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutsStream(tap)
        # Check authenticator generates correct header
        authenticator = stream.authenticator
        # authenticator should be APIKeyAuthenticator with key api-key
        assert authenticator.auth_headers.get("api-key") == SAMPLE_CONFIG["api_key"]

        # Also test that prepared request gets header
        req = requests.Request("GET", "https://api.hevyapp.com/v1/workouts").prepare()
        authed = authenticator(req)
        assert authed.headers.get("api-key") == SAMPLE_CONFIG["api_key"]

    def test_no_api_key_logged(self, caplog):
        # Ensure tap doesn't log api_key; simple check that config secret flag
        tap = TapHevy(config=SAMPLE_CONFIG)
        assert (
            tap.config_jsonschema["properties"]["api_key"].get("secret") is True
            or tap.config_jsonschema["properties"]["api_key"].get("writeOnly") is True
        )


class TestPagination:
    def test_page_number_paginator_has_more(self):
        paginator = HevyPageNumberPaginator(start_value=1)
        # page 1 of 2 has more
        resp = _make_response({"page": 1, "page_count": 2})
        assert paginator.has_more(resp) is True
        # page 2 of 2 has no more
        resp2 = _make_response({"page": 2, "page_count": 2})
        assert paginator.has_more(resp2) is False
        # page 1 of 1 no more
        resp3 = _make_response({"page": 1, "page_count": 1})
        assert paginator.has_more(resp3) is False
        # missing page info -> no more
        resp4 = _make_response({})
        assert paginator.has_more(resp4) is False

    def test_page_number_paginator_get_next(self):
        paginator = HevyPageNumberPaginator(start_value=1)
        # current value 1, next should be 2 if has more
        resp = _make_response({"page": 1, "page_count": 2})
        assert paginator.get_next(resp) == 2
        # if at last page, None
        resp2 = _make_response({"page": 2, "page_count": 2})
        assert paginator.get_next(resp2) is None

    def test_workouts_pagination_termination(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutsStream(tap)
        # Mock two pages
        requests_mock.get(
            "https://api.hevyapp.com/v1/workouts",
            [
                {
                    "json": {"page": 1, "page_count": 2, "workouts": [SAMPLE_WORKOUT]},
                    "status_code": 200,
                },
                {
                    "json": {
                        "page": 2,
                        "page_count": 2,
                        "workouts": [{**SAMPLE_WORKOUT, "id": "id2"}],
                    },
                    "status_code": 200,
                },
            ],
        )
        records = list(stream.get_records(context=None))
        assert len(records) == 2
        assert records[0]["id"] == SAMPLE_WORKOUT["id"]
        assert records[1]["id"] == "id2"
        # Verify only 2 requests made (no extra)
        assert requests_mock.call_count == 2

    def test_pagination_no_extra_request_single_page(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = RoutinesStream(tap)
        requests_mock.get(
            "https://api.hevyapp.com/v1/routines",
            json={"page": 1, "page_count": 1, "routines": [SAMPLE_ROUTINE]},
        )
        records = list(stream.get_records(context=None))
        assert len(records) == 1
        assert requests_mock.call_count == 1

    def test_exercise_templates_pagination(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = ExerciseTemplatesStream(tap)
        requests_mock.get(
            "https://api.hevyapp.com/v1/exercise_templates",
            json={"page": 1, "page_count": 1, "exercise_templates": [SAMPLE_EXERCISE_TEMPLATE]},
        )
        records = list(stream.get_records(context=None))
        assert len(records) == 1
        assert records[0]["id"] == "D04AC939"


class TestRetryHandling:
    def test_429_retry_with_backoff(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutsStream(tap)
        # First request 429, second success
        requests_mock.get(
            "https://api.hevyapp.com/v1/workouts",
            [
                {"status_code": 429, "json": {}, "headers": {"Retry-After": "0"}},
                {
                    "json": {"page": 1, "page_count": 1, "workouts": [SAMPLE_WORKOUT]},
                    "status_code": 200,
                },
            ],
        )
        # Patch backoff to avoid sleep
        with patch("time.sleep", return_value=None):
            records = list(stream.get_records(context=None))
        assert len(records) == 1
        assert requests_mock.call_count == 2

    def test_retry_after_header_parsing(self):
        import email.utils

        tap = TapHevy(config=SAMPLE_CONFIG)
        # Create a dummy stream to test parsing
        stream = WorkoutsStream(tap)
        # Access private helpers via backoff_wait_generator logic
        # We'll test that 429 with Retry-After 5 yields wait 5
        # Simulate exception with response
        resp = _make_response({}, status_code=429, headers={"Retry-After": "5"})
        from singer_sdk.exceptions import RetriableAPIError

        exc = RetriableAPIError("429", resp)
        # Get wait generator and send exception
        gen = stream.backoff_wait_generator()
        # backoff library primes with send(None) or next? Our generator expects send(exception)
        # The SDK's backoff_runtime expects to handle send
        # Let's test by iterating via backoff_runtime protocol
        try:
            next(gen)  # prime
        except StopIteration:
            pass
        # Now send exception
        try:
            wait = gen.send(exc)
            assert wait == 5 or wait == 5.0
        except StopIteration as e:
            # If generator finished, fail
            pytest.fail(f" Generator stopped: {e}")

        # Test HTTP-date Retry-After
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=10)
        http_date = email.utils.format_datetime(future)
        resp2 = _make_response({}, status_code=429, headers={"Retry-After": http_date})
        exc2 = RetriableAPIError("429", resp2)
        gen2 = stream.backoff_wait_generator()
        next(gen2)
        wait2 = gen2.send(exc2)
        # Should be ~10 seconds (allow tolerance)
        assert 8 <= wait2 <= 12

    def test_5xx_retry(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = RoutineFoldersStream(tap)
        requests_mock.get(
            "https://api.hevyapp.com/v1/routine_folders",
            [
                {"status_code": 500, "json": {}},
                {
                    "json": {
                        "page": 1,
                        "page_count": 1,
                        "routine_folders": [SAMPLE_ROUTINE_FOLDER],
                    },
                    "status_code": 200,
                },
            ],
        )
        with patch("time.sleep", return_value=None):
            records = list(stream.get_records(context=None))
        assert len(records) == 1
        assert requests_mock.call_count == 2

    def test_no_retry_on_401(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutsStream(tap)
        requests_mock.get(
            "https://api.hevyapp.com/v1/workouts",
            status_code=401,
            json={"error": "unauthorized"},
        )
        with pytest.raises(Exception):  # noqa: B017
            list(stream.get_records(context=None))
        # Should not retry - only 1 call
        assert requests_mock.call_count == 1


class TestIncremental:
    def test_workout_events_since_param_from_start_date(self):
        tap = TapHevy(
            config={"api_key": SAMPLE_CONFIG["api_key"], "start_date": "2024-01-15T00:00:00Z"}
        )
        stream = WorkoutEventsStream(tap)
        # Simulate sync start to populate starting marker
        stream._write_starting_replication_value(context=None)
        params = stream.get_url_params(context=None, next_page_token=None)
        assert params["since"] == "2024-01-15T00:00:00Z"
        assert params["page"] == 1
        assert params["pageSize"] == 10

    def test_workout_events_since_with_state(self):
        state_dict = {
            "bookmarks": {
                "workout_events": {
                    "replication_key": "event_timestamp",
                    "replication_key_value": "2024-02-01T00:00:00Z",
                    "starting_replication_value": "2024-02-01T00:00:00Z",
                }
            }
        }
        # Set state before creating stream so tap_state reference is correct
        tap = TapHevy(config=SAMPLE_CONFIG, state=state_dict)  # type: ignore[call-arg]
        # Alternative: assign to _state before stream init
        # tap._state is used internally; ensure stream sees it
        stream = WorkoutEventsStream(tap)
        # Simulate sync start to populate starting marker from state/bookmark
        stream._write_starting_replication_value(context=None)
        params = stream.get_url_params(context=None, next_page_token=5)
        # Since state exists, since should be bookmark value
        assert params["since"] == "2024-02-01T00:00:00Z"
        assert params["page"] == 5

    def test_workout_events_post_process_updated(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutEventsStream(tap)
        row = {"type": "updated", "workout": SAMPLE_WORKOUT}
        out = stream.post_process(row, None)
        assert out["id"] == SAMPLE_WORKOUT["id"]
        assert out["event_timestamp"] == SAMPLE_WORKOUT["updated_at"]
        assert out["deleted_at"] is None

    def test_workout_events_post_process_deleted(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutEventsStream(tap)
        row = {"type": "deleted", "id": "deleted-id", "deleted_at": "2024-01-02T12:00:00Z"}
        out = stream.post_process(row, None)
        assert out["id"] == "deleted-id"
        assert out["event_timestamp"] == "2024-01-02T12:00:00Z"
        assert out["workout"] is None

    def test_workout_events_incremental_pagination(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutEventsStream(tap)
        requests_mock.get(
            "https://api.hevyapp.com/v1/workouts/events",
            [
                {"json": SAMPLE_WORKOUT_EVENTS_PAGE1, "status_code": 200},
                {"json": SAMPLE_WORKOUT_EVENTS_PAGE2, "status_code": 200},
            ],
        )
        raw_records = list(stream.get_records(context=None))
        assert len(raw_records) == 3
        records = [stream.post_process(r, None) for r in raw_records]  # type: ignore
        # Check post_processed records have event_timestamp
        assert records[0]["type"] == "updated"  # type: ignore
        assert records[0]["event_timestamp"] == SAMPLE_WORKOUT["updated_at"]  # type: ignore
        assert records[1]["type"] == "deleted"  # type: ignore
        assert records[1]["event_timestamp"] == "2024-01-02T12:00:00Z"  # type: ignore
        assert requests_mock.call_count == 2

    def test_full_table_streams_no_since(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        for cls in [
            WorkoutsStream,
            RoutinesStream,
            ExerciseTemplatesStream,
            RoutineFoldersStream,
            BodyMeasurementsStream,
        ]:
            stream = cls(tap)
            params = stream.get_url_params(context=None, next_page_token=1)
            assert "since" not in params
            assert params["page"] == 1


class TestChildStream:
    def test_exercise_history_parent_context(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        parent = ExerciseTemplatesStream(tap)
        child = ExerciseHistoryStream(tap)
        record = {"id": "D04AC939", "title": "Bench"}
        ctx = parent.get_child_context(record, None)
        assert ctx == {"exercise_template_id": "D04AC939"}
        assert child.parent_stream_type == ExerciseTemplatesStream

    def test_exercise_history_fetch(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = ExerciseHistoryStream(tap)
        requests_mock.get(
            "https://api.hevyapp.com/v1/exercise_history/D04AC939",
            json=SAMPLE_EXERCISE_HISTORY,
        )
        ctx = {"exercise_template_id": "D04AC939"}
        records = list(stream.get_records(context=ctx))
        assert len(records) == 1
        assert records[0]["exercise_template_id"] == "D04AC939"
        assert records[0]["workout_id"] == SAMPLE_WORKOUT["id"]
        assert requests_mock.call_count == 1
        # Ensure URL was constructed with path param
        assert "D04AC939" in requests_mock.request_history[0].url

    def test_exercise_history_no_context(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = ExerciseHistoryStream(tap)
        # Without context, should yield no records and make no request
        records = list(stream.get_records(context=None))
        assert len(records) == 0
        assert requests_mock.call_count == 0

    def test_parent_child_integration(self, requests_mock):
        # Simulate parent fetching and child fetching via tap sync logic
        tap = TapHevy(config=SAMPLE_CONFIG)
        # Mock parent page
        requests_mock.get(
            "https://api.hevyapp.com/v1/exercise_templates",
            json={"page": 1, "page_count": 1, "exercise_templates": [SAMPLE_EXERCISE_TEMPLATE]},
        )
        requests_mock.get(
            "https://api.hevyapp.com/v1/exercise_history/D04AC939",
            json=SAMPLE_EXERCISE_HISTORY,
        )
        parent_stream = ExerciseTemplatesStream(tap)
        parent_records = list(parent_stream.get_records(context=None))
        assert len(parent_records) == 1
        # Now simulate child context generation as tap would do
        child_stream = ExerciseHistoryStream(tap)
        ctx = parent_stream.get_child_context(parent_records[0], None)
        child_records = list(child_stream.get_records(context=ctx))
        assert len(child_records) == 1


class TestSchemas:
    def test_schemas_valid(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        for stream in tap.discover_streams():
            schema = stream.schema
            # Basic JSON schema validity: must have type object and properties
            assert "type" in schema
            assert "properties" in schema
            # Check primary keys are in schema
            for pk in stream.primary_keys:
                assert (
                    pk in schema["properties"] or pk == "event_timestamp"
                )  # event_timestamp is derived

    def test_user_info_schema(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = UserInfoStream(tap)
        assert stream.primary_keys == ("id",)
        assert stream.records_jsonpath == "$.data"
        # Schema should have id required
        assert "id" in stream.schema["properties"]
        assert "id" in stream.schema.get("required", [])

    def test_workouts_schema_nested(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutsStream(tap)
        # Check nested exercises array exists
        assert "exercises" in stream.schema["properties"]
        ex_prop = stream.schema["properties"]["exercises"]
        assert ex_prop["type"] in (["array", "null"], "array")

    def test_workout_events_schema(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutEventsStream(tap)
        assert stream.replication_key == "event_timestamp"
        assert "event_timestamp" in stream.schema["properties"]
        assert "workout" in stream.schema["properties"]

    def test_body_measurements_schema(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = BodyMeasurementsStream(tap)
        assert stream.primary_keys == ("date",)
        assert "date" in stream.schema["properties"]
        assert "weight_kg" in stream.schema["properties"]

    def test_representative_responses_parse(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        # Test each paginated stream with representative JSON
        mocks = [
            (
                WorkoutsStream,
                "https://api.hevyapp.com/v1/workouts",
                {"page": 1, "page_count": 1, "workouts": [SAMPLE_WORKOUT]},
            ),
            (
                RoutinesStream,
                "https://api.hevyapp.com/v1/routines",
                {"page": 1, "page_count": 1, "routines": [SAMPLE_ROUTINE]},
            ),
            (
                ExerciseTemplatesStream,
                "https://api.hevyapp.com/v1/exercise_templates",
                {"page": 1, "page_count": 1, "exercise_templates": [SAMPLE_EXERCISE_TEMPLATE]},
            ),
            (
                RoutineFoldersStream,
                "https://api.hevyapp.com/v1/routine_folders",
                {"page": 1, "page_count": 1, "routine_folders": [SAMPLE_ROUTINE_FOLDER]},
            ),
            (
                BodyMeasurementsStream,
                "https://api.hevyapp.com/v1/body_measurements",
                {"page": 1, "page_count": 1, "body_measurements": [SAMPLE_BODY]},
            ),
        ]
        for cls, url, payload in mocks:
            requests_mock.reset()
            requests_mock.get(url, json=payload)
            stream = cls(tap)
            records = list(stream.get_records(context=None))
            assert len(records) == 1

    def test_user_info_parse(self, requests_mock):
        tap = TapHevy(config=SAMPLE_CONFIG)
        requests_mock.get(
            "https://api.hevyapp.com/v1/user/info",
            json={"data": SAMPLE_USER},
        )
        stream = UserInfoStream(tap)
        records = list(stream.get_records(context=None))
        assert len(records) == 1
        assert records[0]["id"] == SAMPLE_USER["id"]


class TestTimeoutAndConfig:
    def test_configurable_timeout(self):
        tap = TapHevy(config={**SAMPLE_CONFIG, "request_timeout": 60})
        stream = WorkoutsStream(tap)
        assert stream.timeout == 60
        tap2 = TapHevy(config=SAMPLE_CONFIG)
        stream2 = WorkoutsStream(tap2)
        assert stream2.timeout == 30  # default

    def test_url_base_override(self):
        tap = TapHevy(config={**SAMPLE_CONFIG, "api_url": "https://custom.example.com"})
        stream = WorkoutsStream(tap)
        assert stream.url_base == "https://custom.example.com"

    def test_default_url_base(self):
        tap = TapHevy(config=SAMPLE_CONFIG)
        stream = WorkoutsStream(tap)
        assert stream.url_base == "https://api.hevyapp.com"
