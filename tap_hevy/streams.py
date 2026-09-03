"""Stream type classes for tap-hevy."""

from __future__ import annotations

import sys
import typing as t

import requests
from singer_sdk import typing as th
from singer_sdk.pagination import SinglePagePaginator

from tap_hevy.client import HevyStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# Shared type definitions

SET_OBJECT = th.ObjectType(
    th.Property("index", th.IntegerType, description="Index order of set"),
    th.Property("type", th.StringType, description="Type of set: normal, warmup, dropset, failure"),
    th.Property("weight_kg", th.NumberType),
    th.Property("reps", th.NumberType),
    th.Property("distance_meters", th.NumberType),
    th.Property("duration_seconds", th.NumberType),
    th.Property("rpe", th.NumberType),
    th.Property("custom_metric", th.NumberType),
)

ROUTINE_SET_OBJECT = th.ObjectType(
    th.Property("index", th.IntegerType),
    th.Property("type", th.StringType),
    th.Property("weight_kg", th.NumberType),
    th.Property("reps", th.NumberType),
    th.Property(
        "rep_range",
        th.ObjectType(
            th.Property("start", th.NumberType),
            th.Property("end", th.NumberType),
        ),
    ),
    th.Property("distance_meters", th.NumberType),
    th.Property("duration_seconds", th.NumberType),
    th.Property("rpe", th.NumberType),
    th.Property("custom_metric", th.NumberType),
)

EXERCISE_OBJECT = th.ObjectType(
    th.Property("index", th.IntegerType),
    th.Property("title", th.StringType),
    th.Property("notes", th.StringType),
    th.Property("exercise_template_id", th.StringType),
    th.Property("supersets_id", th.IntegerType),
    th.Property("sets", th.ArrayType(SET_OBJECT)),
)

ROUTINE_EXERCISE_OBJECT = th.ObjectType(
    th.Property("index", th.IntegerType),
    th.Property("title", th.StringType),
    th.Property("rest_seconds", th.StringType),
    th.Property("notes", th.StringType),
    th.Property("exercise_template_id", th.StringType),
    th.Property("supersets_id", th.IntegerType),
    th.Property("sets", th.ArrayType(ROUTINE_SET_OBJECT)),
)


class UserInfoStream(HevyStream):
    """User info stream - single record."""

    name = "user_info"
    path = "/v1/user/info"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.data"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    @override
    def get_new_paginator(self):
        return SinglePagePaginator()

    @override
    def get_url_params(
        self, context: Context | None, next_page_token: int | None
    ) -> dict[str, t.Any]:
        # No pagination params for this endpoint
        return {}


class WorkoutsStream(HevyStream):
    """Workouts stream - paginated list, full table."""

    name = "workouts"
    path = "/v1/workouts"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.workouts[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("routine_id", th.StringType),
        th.Property("start_time", th.DateTimeType),
        th.Property("end_time", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property(
            "exercises",
            th.ArrayType(EXERCISE_OBJECT),
        ),
    ).to_dict()

    @override
    def get_url_params(
        self, context: Context | None, next_page_token: int | None
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}
        params["page"] = next_page_token if next_page_token is not None else 1
        params["pageSize"] = self.get_page_size(max_size=10)
        return params


class WorkoutEventsStream(HevyStream):
    """Workout events - incremental via since param."""

    name = "workout_events"
    path = "/v1/workouts/events"
    primary_keys = ("id", "event_timestamp")
    replication_key = "event_timestamp"
    records_jsonpath = "$.events[*]"
    # API returns newest to oldest (desc), not sorted ascending
    # Replication method will be INCREMENTAL due to replication_key

    schema = th.PropertiesList(
        th.Property("type", th.StringType, required=True, description="updated or deleted"),
        th.Property("id", th.StringType, required=True, description="Workout ID"),
        th.Property("deleted_at", th.DateTimeType),
        th.Property(
            "workout",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("title", th.StringType),
                th.Property("description", th.StringType),
                th.Property("routine_id", th.StringType),
                th.Property("start_time", th.DateTimeType),
                th.Property("end_time", th.DateTimeType),
                th.Property("updated_at", th.DateTimeType),
                th.Property("created_at", th.DateTimeType),
                th.Property(
                    "exercises",
                    th.ArrayType(EXERCISE_OBJECT),
                ),
            ),
        ),
        th.Property("event_timestamp", th.DateTimeType, required=True),
    ).to_dict()

    @override
    def get_url_params(
        self, context: Context | None, next_page_token: int | None
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}
        params["page"] = next_page_token if next_page_token is not None else 1
        params["pageSize"] = self.get_page_size(max_size=10)
        # Use state bookmark or start_date
        starting = self.get_starting_timestamp(context)
        if starting:
            # starting is datetime, convert to ISO
            if hasattr(starting, "isoformat"):
                # Ensure UTC Z format
                iso = starting.isoformat().replace("+00:00", "Z")
                params["since"] = iso
            else:
                params["since"] = str(starting)
        else:
            # Default epoch as per API docs
            params["since"] = "1970-01-01T00:00:00Z"
        return params

    @override
    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        # Normalize record to have top-level id, deleted_at, workout, event_timestamp
        # Input row is either {"type":"updated","workout":{...}}
        # or {"type":"deleted","id":..., "deleted_at":...}
        rtype = row.get("type")
        if rtype == "updated":
            workout = row.get("workout") or {}
            workout_id = workout.get("id")
            row["id"] = workout_id
            # deleted_at stays None
            row["deleted_at"] = None
            # event_timestamp from workout.updated_at or created_at or start_time
            ts = workout.get("updated_at") or workout.get("created_at") or workout.get("start_time")
            row["event_timestamp"] = ts
            # Ensure workout field is preserved
            row["workout"] = workout
        elif rtype == "deleted":
            # id already present, workout should be None
            row["workout"] = None
            row["event_timestamp"] = row.get("deleted_at")
            # ensure deleted_at stays
        else:
            # Unknown type, try to synthesize
            if "workout" in row and isinstance(row["workout"], dict):
                wid = row["workout"].get("id")
                row["id"] = row.get("id") or wid
                row["event_timestamp"] = row["workout"].get("updated_at")
            else:
                row["event_timestamp"] = row.get("deleted_at")
        return row

    @override
    def get_new_paginator(self):
        from tap_hevy.client import HevyPageNumberPaginator

        return HevyPageNumberPaginator(start_value=1)


class RoutinesStream(HevyStream):
    """Routines stream."""

    name = "routines"
    path = "/v1/routines"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.routines[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("title", th.StringType),
        th.Property("folder_id", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property(
            "exercises",
            th.ArrayType(ROUTINE_EXERCISE_OBJECT),
        ),
    ).to_dict()

    @override
    def get_url_params(
        self, context: Context | None, next_page_token: int | None
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}
        params["page"] = next_page_token if next_page_token is not None else 1
        params["pageSize"] = self.get_page_size(max_size=10)
        return params


class ExerciseTemplatesStream(HevyStream):
    """Exercise templates stream."""

    name = "exercise_templates"
    path = "/v1/exercise_templates"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.exercise_templates[*]"

    @override
    def get_child_context(self, record: dict, context: Context | None) -> dict | None:
        return {"exercise_template_id": record.get("id")}

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("title", th.StringType),
        th.Property("type", th.StringType),
        th.Property("primary_muscle_group", th.StringType),
        th.Property(
            "secondary_muscle_groups",
            th.ArrayType(th.StringType),
        ),
        th.Property("equipment_category", th.StringType),
        th.Property("is_custom", th.BooleanType),
    ).to_dict()

    @override
    def get_url_params(
        self, context: Context | None, next_page_token: int | None
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}
        params["page"] = next_page_token if next_page_token is not None else 1
        params["pageSize"] = self.get_page_size(max_size=100)
        return params


class RoutineFoldersStream(HevyStream):
    """Routine folders stream."""

    name = "routine_folders"
    path = "/v1/routine_folders"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.routine_folders[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("index", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
    ).to_dict()

    @override
    def get_url_params(
        self, context: Context | None, next_page_token: int | None
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}
        params["page"] = next_page_token if next_page_token is not None else 1
        params["pageSize"] = self.get_page_size(max_size=10)
        return params


class BodyMeasurementsStream(HevyStream):
    """Body measurements stream."""

    name = "body_measurements"
    path = "/v1/body_measurements"
    primary_keys = ("date",)
    replication_key = None
    records_jsonpath = "$.body_measurements[*]"

    schema = th.PropertiesList(
        th.Property("date", th.StringType, required=True),
        th.Property("weight_kg", th.NumberType),
        th.Property("lean_mass_kg", th.NumberType),
        th.Property("fat_percent", th.NumberType),
        th.Property("neck_cm", th.NumberType),
        th.Property("shoulder_cm", th.NumberType),
        th.Property("chest_cm", th.NumberType),
        th.Property("left_bicep_cm", th.NumberType),
        th.Property("right_bicep_cm", th.NumberType),
        th.Property("left_forearm_cm", th.NumberType),
        th.Property("right_forearm_cm", th.NumberType),
        th.Property("abdomen", th.NumberType),
        th.Property("waist", th.NumberType),
        th.Property("hips", th.NumberType),
        th.Property("left_thigh", th.NumberType),
        th.Property("right_thigh", th.NumberType),
        th.Property("left_calf", th.NumberType),
        th.Property("right_calf", th.NumberType),
    ).to_dict()

    @override
    def get_url_params(
        self, context: Context | None, next_page_token: int | None
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}
        params["page"] = next_page_token if next_page_token is not None else 1
        params["pageSize"] = self.get_page_size(max_size=10)
        return params


class ExerciseHistoryStream(HevyStream):
    """Exercise history - child of exercise_templates."""

    name = "exercise_history"
    path = "/v1/exercise_history/{exercise_template_id}"
    primary_keys = ("exercise_template_id", "workout_id", "workout_start_time")
    replication_key = None
    records_jsonpath = "$.exercise_history[*]"
    parent_stream_type = ExerciseTemplatesStream

    schema = th.PropertiesList(
        th.Property("exercise_template_id", th.StringType, required=True),
        th.Property("workout_id", th.StringType),
        th.Property("workout_title", th.StringType),
        th.Property("workout_start_time", th.DateTimeType),
        th.Property("workout_end_time", th.DateTimeType),
        th.Property("weight_kg", th.NumberType),
        th.Property("reps", th.IntegerType),
        th.Property("distance_meters", th.IntegerType),
        th.Property("duration_seconds", th.IntegerType),
        th.Property("rpe", th.NumberType),
        th.Property("custom_metric", th.NumberType),
        th.Property("set_type", th.StringType),
    ).to_dict()

    @override
    def get_new_paginator(self):
        return SinglePagePaginator()

    @override
    def get_url_params(
        self, context: Context | None, next_page_token: int | None
    ) -> dict[str, t.Any]:
        # exercise_history does not use pagination but supports start_date/end_date
        params: dict[str, t.Any] = {}
        # Optionally filter by start_date from config/state if incremental desired
        # For now return empty; child streams still need page token handling ignored
        # If we want incremental, we could use replication_key workout_start_time
        # and pass start_date derived from state
        return params

    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        # Use parent parse but ensure each record includes exercise_template_id from context?
        # The response already includes exercise_template_id per entry
        data = response.json()
        history = data.get("exercise_history", [])
        yield from history

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        # Ensure context has exercise_template_id
        if context is None or "exercise_template_id" not in context:
            return
        yield from super().get_records(context)
