from typing import TypedDict, Optional
import numpy as np


class VariableSpec(TypedDict, total=False):
    # REQUIRED (for Dataset construction)
    dtype: np.dtype

    # OPTIONAL (for decoding DB values)
    scale: Optional[float]
    add_offset: Optional[float]

    # OPTIONAL (for NetCDF)
    fill_value: Optional[float]
    attrs: dict[str, str | float | int]

