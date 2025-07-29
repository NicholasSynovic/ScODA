"""
Generate water pressure data.

Copyright 2025 (C) Nicholas M. Synovic

"""

import argparse

import numpy as np
import pandas as pd


def load_pressure_data(file_path: str) -> pd.DataFrame:
    """
    Load and preprocess InfluxDB-style water pressure data.

    This function reads a whitespace-delimited text file in InfluxDB line
    protocol format, extracts relevant fields, and returns a structured
    DataFrame with cleaned values.

    Arguments:
        file_path: Path to the input text or CSV file.

    Returns:
        A pandas DataFrame containing the timestamp, cname, and value columns
        parsed and cast to appropriate types.

    """
    df = pd.read_csv(
        file_path,
        header=None,
        names=["metric", "cname", "units", "value", "timestamp_ns"],
        delim_whitespace=True,
    )

    # Extract values from key=value pairs
    df["cname"] = df["cname"].str.split("=").str[1]
    df["units"] = df["units"].str.split("=").str[1]
    df["value"] = df["value"].str.split("=").str[1].astype(float)
    df["timestamp"] = pd.to_datetime(
        df["timestamp_ns"].astype(np.int64),
        unit="ns",
    )

    return df[["timestamp", "cname", "value"]]


def simulate_pressure_data(  # noqa: PLR0913, PLR0917
    total_seconds: int,
    lambda_idle: float,
    lambda_use: float,
    idle_mean: float,
    idle_std: float,
    use_mean: float,
    use_std: float,
    sampling_interval: int,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Simulate synthetic water pressure data alternating.

    This function generates time-series pressure values over a given duration by
    switching between idle and active usage states, with durations sampled from
    exponential distributions and pressure values sampled from normal
    distributions defined per state.

    Arguments:
        total_seconds: Total duration of the simulation in seconds.
        lambda_idle: Rate (1/mean seconds) of the idle state.
        lambda_use: Rate (1/mean seconds) of the use state.
        idle_mean: Mean pressure value during idle state.
        idle_std: Standard deviation of pressure during idle state.
        use_mean: Mean pressure value during use state.
        use_std: Standard deviation of pressure during use state.
        sampling_interval: Time interval between samples (in seconds).
        seed: Seed for the random number generator.

    Returns:
        A pandas DataFrame with timestamps and simulated pressure values.

    """
    rng: np.random.Generator = np.random.Generator(np.random.PCG64(seed=seed))

    timestamps = []
    pressures = []

    current_time = 0
    state = 0  # Start as idle

    while current_time < total_seconds:
        if state == 0:
            duration = rng.exponential(1 / lambda_idle)
            mean, std = idle_mean, idle_std
        else:
            duration = rng.exponential(1 / lambda_use)
            mean, std = use_mean, use_std

        duration = int(
            min(duration, (total_seconds - current_time) / sampling_interval)
        )

        for _ in range(duration):
            timestamps.append(current_time)
            pressures.append(rng.normal(mean, std))
            current_time += sampling_interval

        state = 1 - state  # toggle state

    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                timestamps,
                unit="s",
                origin="2024-01-01",
            ),
            "simulated_pressure_psig": np.round(pressures, 2),
        }
    )


def main() -> None:
    """
    Run the pressure data simulation workflow using real input data.

    This function parses command-line arguments, loads real pressure data from a
    CSV file, estimates statistical parameters for idle and use states,
    generates simulated pressure data using those parameters, and writes the
    simulated data to an output CSV file.

    Arguments:
        None. Command-line arguments are parsed internally.

    """
    parser = argparse.ArgumentParser(
        description="Simulate pressure event data based on real sensor input."
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input CSV file with real pressure data.",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=3600,
        help="Total simulation duration in seconds.",
    )
    parser.add_argument(
        "--lambda-idle",
        type=float,
        default=1 / 300,
        help="Rate of idle state (1/mean duration).",
    )
    parser.add_argument(
        "--lambda-use",
        type=float,
        default=1 / 120,
        help="Rate of use state (1/mean duration).",
    )
    parser.add_argument(
        "--sampling-interval",
        type=int,
        default=1,
        help="Sampling interval in seconds.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument(
        "--output",
        type=str,
        default="simulated_pressure.csv",
        help="Output CSV file.",
    )

    args = parser.parse_args()

    real_df = load_pressure_data(args.input)

    # Use the min/max split to guess idle vs use pressure
    midpoint = real_df["value"].mean()
    idle_values = real_df[real_df["value"] < midpoint]["value"]
    use_values = real_df[real_df["value"] >= midpoint]["value"]

    df_sim = simulate_pressure_data(
        total_seconds=args.duration,
        lambda_idle=args.lambda_idle,
        lambda_use=args.lambda_use,
        idle_mean=idle_values.mean(),
        idle_std=idle_values.std(),
        use_mean=use_values.mean(),
        use_std=use_values.std(),
        sampling_interval=args.sampling_interval,
        seed=args.seed,
    )

    df_sim.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
