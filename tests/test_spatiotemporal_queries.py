import numpy as np
from datetime import datetime
from OceanDB.data_access.along_track import AlongTrack

def test_geographic_points_in_r_dt():
    along_track = AlongTrack()

    """
    TEST single point spatiotemporal query
    """

    latitude = -69
    longitude = 28.1
    date = datetime(year=2019, month=1, day=1, hour=1)

    fields = list(AlongTrack.schema.keys())
    along_track_query_result_iterator = along_track.geographic_points_in_r_dt(
        latitudes=np.array([latitude]), longitudes=np.array([longitude]), dates=[date], fields=fields
    )

    along_track_output_list = list(along_track_query_result_iterator)
    result = along_track_output_list[0]
    for field in fields:
        assert field in result
    assert result['filename'] == 'asdfasdf'
