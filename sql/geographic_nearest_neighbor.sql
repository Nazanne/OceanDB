SELECT
	 longitude,
     latitude,
	 sla_filtered,
	 EXTRACT(EPOCH FROM ({central_date_time} - date_time)) AS time_difference_secs,
     along_track_point <-> ST_MakePoint(%(longitude)s, %(latitude)s) AS distance
FROM along_track
WHERE date_time BETWEEN {central_date_time} - {time_delta} AND {central_date_time} + {time_delta}
AND (basin_id = %(basin_id)s OR basin_id = ANY( %(connected_basin_ids)s ))
AND SPLIT_PART(file_name, '_', 3) IN ({missions})
ORDER BY distance
LIMIT 3