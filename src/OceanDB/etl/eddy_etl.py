from dataclasses import dataclass
from dataclasses import asdict
import netCDF4 as nc
import pandas as pd
import psycopg
import psycopg as pg
from psycopg import sql
import glob
import time
import os
import numpy as np
from OceanDB.OceanDB import OceanDB
from functools import cached_property
from typing import List, Tuple, Any, Iterable, Optional, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from OceanDB.utils.postgres_upsert import upsert_ignore


NDArray = np.ndarray


@dataclass
class EddyData:
    """Structured container for detected eddy observations."""
    amplitude: NDArray
    cost_association: NDArray
    effective_area: NDArray
    effective_contour_height: NDArray
    effective_contour_latitude: NDArray
    effective_contour_longitude: NDArray
    effective_contour_shape_error: NDArray
    effective_radius: NDArray
    inner_contour_height: NDArray
    latitude: NDArray
    latitude_max: NDArray
    longitude: NDArray
    longitude_max: NDArray
    num_contours: NDArray
    num_point_e: NDArray
    num_point_s: NDArray
    observation_flag: NDArray  # will normalize to bool
    observation_number: NDArray
    speed_area: NDArray
    speed_average: NDArray
    speed_contour_height: NDArray
    speed_contour_latitude: NDArray
    speed_contour_longitude: NDArray
    speed_contour_shape_error: NDArray
    speed_radius: NDArray
    date_time: NDArray
    track: NDArray

    def __post_init__(self) -> None:
        """Normalize and validate eddy data arrays."""
        if self.observation_flag.dtype != bool:
            self.observation_flag = self.observation_flag.astype(bool)

        # Basic shape validation (cheap, very effective)
        n = len(self.latitude)
        for name, value in vars(self).items():
            if len(value) != n:
                raise ValueError(
                    f"EddyData field '{name}' has length {len(value)} != {n}"
                )

