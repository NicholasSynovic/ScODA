"""
Generate data for InfluxDB.

Copyright 2025 (C) Nicholas M. Synovic

"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from string import Template

import numpy as np


def simulate_status_data(  # noqa: PLR0913, PLR0917
    total_seconds: int,
    lambda_idle: float,
    lambda_use: float,
    sampling_interval: int,
    seed: int,
    output_file: str,
    base_time: str,
    measurement: str,
    tag_key: str,
    tag_value: str,
    unit: str,
    field_key: str,
) -> None:
    """
    Simulate binary status data and write it in InfluxDB line protocol format.

    This function generates time-series status data that alternates between idle (0)
    and in-use (1) states. The duration of each state is sampled from an exponential
    distribution parameterized by lambda values. The resulting records are written to
    a file in InfluxDB line protocol format with nanosecond-precision timestamps.

    Arguments:
        total_seconds: Total duration of the simulation in seconds.
        lambda_idle: Rate (1/mean seconds) for the idle state duration.
        lambda_use: Rate (1/mean seconds) for the in-use state duration.
        sampling_interval: Time interval between successive data samples (in seconds).
        seed: Random seed for reproducibility.
        output_file: Path to the file where output data will be written.
        base_time: Base timestamp in ISO 8601 format (e.g., "2024-01-01T00:00:00").
        measurement: Measurement name for InfluxDB.
        tag_key: Tag key to associate with each data point.
        tag_value: Tag value corresponding to the tag key.
        unit: Value for the "units" tag in the line protocol.
        field_key: Field key for the data value (e.g., "value").

    """
    rng: np.random.Generator = np.random.Generator(np.random.PCG64(seed=seed))

    current_time = 0
    state = 0  # 0 for idle, 1 for in-use

    records = []
    base_dt = datetime.fromisoformat(base_time)

    line_template: Template = Template(
        template="${measurement},${tag_key}=${tag_value},units=${unit} ${field_key}=${state}i ${ns_timestamp}}"  # noqa: E501
    )

    while current_time < total_seconds:
        duration = rng.exponential(1 / (lambda_idle if state == 0 else lambda_use))
        steps = int(min(duration, (total_seconds - current_time) / sampling_interval))

        for _ in range(steps):
            timestamp = base_dt + timedelta(seconds=current_time)
            ns_timestamp = int(timestamp.timestamp() * 1e9)
            line = line_template.substitute(
                measurement=measurement,
                tag_key=tag_key,
                tag_value=tag_value,
                unit=unit,
                field_key=field_key,
                state=state,
                ns_timestamp=ns_timestamp,
            )
            records.append(line)
            current_time += sampling_interval

        state = 1 - state

    Path(output_file).write_text(data="\n".join(records), encoding="utf8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate InfluxDB-style status event data."
    )
    parser.add_argument(
        "--duration", type=int, default=3600, help="Total duration in seconds."
    )
    parser.add_argument(
        "--lambda-idle", type=float, default=1 / 300, help="Idle rate (1/mean seconds)."
    )
    parser.add_argument(
        "--lambda-use", type=float, default=1 / 120, help="Use rate (1/mean seconds)."
    )
    parser.add_argument(
        "--sampling-interval", type=int, default=1, help="Sampling interval in seconds."
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument(
        "--output", type=str, default="status_events.txt", help="Output file."
    )
    parser.add_argument(
        "--base-time",
        type=str,
        default="2024-01-01T00:00:00",
        help="Base timestamp (ISO format).",
    )
    parser.add_argument(
        "--measurement",
        type=str,
        default="BC_H_ARIES_VRM_FLT",
        help="Influx measurement name.",
    )
    parser.add_argument("--tag-key", type=str, default="cname", help="Tag key.")
    parser.add_argument("--tag-value", type=str, default="c0-0c0s0", help="Tag value.")
    parser.add_argument("--unit", type=str, default="status", help="Units tag.")
    parser.add_argument(
        "--field-key", type=str, default="value", help="Field key (e.g., value)."
    )

    args = parser.parse_args()

    simulate_status_data(
        total_seconds=args.duration,
        lambda_idle=args.lambda_idle,
        lambda_use=args.lambda_use,
        sampling_interval=args.sampling_interval,
        seed=args.seed,
        output_file=args.output,
        base_time=args.base_time,
        measurement=args.measurement,
        tag_key=args.tag_key,
        tag_value=args.tag_value,
        unit=args.unit,
        field_key=args.field_key,
    )
