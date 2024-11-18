CREATE INDEX IF NOT EXISTS chelton_eddy_point_idx
            ON public.{table_name} USING gist
            (chelton_eddy_point)
            WITH (buffering=auto)
            TABLESPACE pg_default;