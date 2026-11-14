"""Features (past steps only) and the synthetic wall-contact label (future steps), ADR 0020."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

WAREHOUSE_M = 50.0  # simulator constant (fleetforge.simulator.fleet.WAREHOUSE_M)
HORIZON = 30
EDGE_M = 0.01  # within 1 cm of a wall counts as contact
FEATURES = [
    "dist_wall",
    "dist_x",
    "dist_y",
    "speed",
    "vx",
    "vy",
    "vx_mean5",
    "vy_mean5",
    "toward_wall",
    "battery_pct",
]
LABEL = f"wall_contact_next_{HORIZON}"


def load(export_dir: Path, split_column: str = "split") -> dict[str, pd.DataFrame]:
    frames = [pd.read_parquet(p) for p in sorted(export_dir.glob("*.parquet"))]
    data = add_label(add_features(pd.concat(frames, ignore_index=True)))
    return {name: part.reset_index(drop=True) for name, part in data.groupby(split_column)}


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["episode_id", "sequence_number"]).reset_index(drop=True)
    dx = np.minimum(df["x_m"], WAREHOUSE_M - df["x_m"])
    dy = np.minimum(df["y_m"], WAREHOUSE_M - df["y_m"])
    df["dist_x"], df["dist_y"], df["dist_wall"] = dx, dy, np.minimum(dx, dy)
    df["vx"] = df["vx_mps"].fillna(0.0)
    df["vy"] = df["vy_mps"].fillna(0.0)
    df["speed"] = np.hypot(df["vx"], df["vy"])
    by_episode = df.groupby("episode_id")
    df["vx_mean5"] = by_episode["vx"].transform(lambda s: s.rolling(5, min_periods=1).mean())
    df["vy_mean5"] = by_episode["vy"].transform(lambda s: s.rolling(5, min_periods=1).mean())
    # Positive when the recent motion points at the nearest wall.
    sx = np.where(df["x_m"] < WAREHOUSE_M / 2, -1.0, 1.0) * df["vx_mean5"]
    sy = np.where(df["y_m"] < WAREHOUSE_M / 2, -1.0, 1.0) * df["vy_mean5"]
    df["toward_wall"] = np.where(dx < dy, sx, sy)
    return df


def add_label(df: pd.DataFrame) -> pd.DataFrame:
    """Label from the next HORIZON steps; rows without a full horizon or already at a wall go."""
    touch = (df["dist_wall"] <= EDGE_M).astype(int)
    by_episode = touch.groupby(df["episode_id"])
    future = by_episode.transform(
        lambda s: s[::-1].rolling(HORIZON, min_periods=HORIZON).max()[::-1].shift(-1)
    )
    df[LABEL] = future
    keep = df[LABEL].notna() & (df["dist_wall"] > EDGE_M)
    return df[keep].astype({LABEL: int})
