from dataclasses import dataclass


@dataclass
class OceanDataField:
    nc_name: str
    nc_scale: int
    nc_offset: int
    python_type: type
    postgres_type: str
    postgres_column_or_query_name: str
