"""TapHevy tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_hevy import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapHevy(Tap):
    """Singer tap for Hevy."""

    name = "tap-hevy"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            title="API Key",
            description=(
                "Hevy API key (UUID) from https://hevy.com/settings?developer. "
                "Requires Hevy Pro."
            ),
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.hevyapp.com",
            title="API URL",
            description="Base URL for Hevy API. Defaults to https://api.hevyapp.com",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Start date for incremental streams (e.g., workout_events).",
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=30,
            title="Request Timeout",
            description="Timeout in seconds for HTTP requests.",
        ),
        th.Property(
            "max_retries",
            th.IntegerType,
            default=5,
            title="Max Retries",
            description="Maximum number of retries for retriable errors.",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            description="User agent string for HTTP requests.",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.HevyStream]:
        return [
            streams.UserInfoStream(self),
            streams.WorkoutsStream(self),
            streams.WorkoutEventsStream(self),
            streams.RoutinesStream(self),
            streams.ExerciseTemplatesStream(self),
            streams.RoutineFoldersStream(self),
            streams.BodyMeasurementsStream(self),
            streams.ExerciseHistoryStream(self),
        ]


if __name__ == "__main__":
    TapHevy.cli()
