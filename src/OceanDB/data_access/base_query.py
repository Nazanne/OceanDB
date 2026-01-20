from OceanDB.OceanDB import OceanDB
from OceanDB.data_access.metadata import METADATA_REGISTRY


class BaseQuery(OceanDB):
    METADATA = METADATA_REGISTRY