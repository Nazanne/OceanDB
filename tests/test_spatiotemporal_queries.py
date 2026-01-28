import numpy as np
import numpy.typing as npt
from datetime import datetime, timedelta
from OceanDB.data_access.along_track import AlongTrack

def test_geographic_points_in_r_dt():
    along_track = AlongTrack()

    """
    TEST single point spatiotemporal query
    """

    latitude = -69
    longitude = 28.1
    date = datetime(year=2019, month=1, day=1, hour=1)
    radius = 500_000
    time_window = timedelta(days=10)

    fields = list(AlongTrack.schema.keys())
    along_track_query_result_iterator = along_track.geographic_points_in_r_dt(
        latitudes=np.array([latitude]), longitudes=np.array([longitude]), dates=[date], fields=fields, radii=radius, time_window = time_window,
    )

    along_track_output_list = list(along_track_query_result_iterator)
    result = along_track_output_list[0]
    assert result is not None
    for field in fields:
        assert field in result
        # assert result[field].dtype == result.schema[field].python_type
    assert "dt_global_alg_phy_l3_1hz_20190102_20240205.nc" in result['file_name']
    assert ((result["date_time"] - date) <= time_window).all()
    assert (result['distance'] <= radius).all()
