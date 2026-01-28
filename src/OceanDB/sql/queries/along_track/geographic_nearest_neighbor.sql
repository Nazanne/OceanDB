SELECT
{fields}
FROM along_track
WHERE date_time BETWEEN %(central_date_time)s - %(time_delta)s::interval
                    AND %(central_date_time)s + %(time_delta)s::interval
  AND basin_id = ANY(%(connected_basin_ids)s)
  AND mission = ANY(%(missions)s)
ORDER BY distance
LIMIT 3;
