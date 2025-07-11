import argparse
import numpy as np
from datetime import datetime, timedelta


def simulate_status_data(
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
):
    np.random.seed(seed)

    current_time = 0
    state = 0  # 0 for idle, 1 for in-use

    records = []
    base_dt = datetime.fromisoformat(base_time)

    while current_time < total_seconds:
        duration = np.random.exponential(
            1 / (lambda_idle if state == 0 else lambda_use)
        )
        steps = int(min(duration, (total_seconds - current_time) / sampling_interval))

        for _ in range(steps):
            timestamp = base_dt + timedelta(seconds=current_time)
            ns_timestamp = int(timestamp.timestamp() * 1e9)
            line = f"{measurement},{tag_key}={tag_value},units={unit} {field_key}={state}i {ns_timestamp}"
            records.append(line)
            current_time += sampling_interval

        state = 1 - state

    with open(output_file, "w") as f:
        f.write("\n".join(records))

    print(f"Wrote {len(records)} records to {output_file}")


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
