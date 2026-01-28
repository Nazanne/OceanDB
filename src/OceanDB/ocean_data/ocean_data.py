from dataclasses import dataclass
import psycopg.sql as sql


@dataclass
class OceanDataField:
    nc_name: str
    nc_scale: int
    nc_offset: int
    python_type: type
    postgres_type: str
    postgres_column_or_query_name: str
    custom_calculation: str | None = None

    def to_sql_query(self):
        output_name = sql.Identifier(self.postgres_column_or_query_name)
        if self.custom_calculation:
            postgres_calc = sql.SQL(self.custom_calculation)
        else:
            postgres_calc = sql.Identifier(self.postgres_column_or_query_name)

        return sql.SQL("{postgres_calc} AS {output_name}").format(
                postgres_calc=postgres_calc,
                output_name=output_name
            )
