from datetime import datetime, timedelta
from typing import Iterable, List, Literal, get_args
import psycopg as pg
import numpy.typing as npt
import numpy as np

from OceanDB.data_access.base_query import BaseQuery
from OceanDB.data_access.schema.along_track_schema import along_track_fields, along_track_schema
from OceanDB.ocean_data.dataset import Dataset


class Eddy(BaseQuery):
    pass