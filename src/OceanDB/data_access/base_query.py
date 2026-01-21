from OceanDB.OceanDB import OceanDB
from OceanDB.data_access.metadata import METADATA_REGISTRY
from OceanDB.ocean_data.ocean_data import OceanData, Dataset


class BaseQuery(OceanDB):
    METADATA = METADATA_REGISTRY

    def build_dataset(
        self,
        *,
        dataset_cls: Dataset,
        rows,
        domain: str,
    )->Dataset:
        """
        Construct a Dataset instance from raw database rows using
        domain-registered metadata.

        This method:
        - Resolves variable metadata via the global metadata registry
          using the provided domain key (e.g. "eddy", "along_track")
        - Applies dtype coercion, scaling, and offsets
        - Returns a fully materialized Dataset ready for bundling
          into an OceanData container

        Parameters
        ----------
        dataset_cls : type[Dataset]
            Concrete Dataset subclass to construct (e.g. EddyDataset,
            AlongTrackDataset).

        rows : Sequence[Mapping[str, Any]]
            Raw rows returned from a database query, typically using
            psycopg's dict_row factory.

        domain : str
            Metadata domain key used to resolve variable definitions
            from the metadata registry.

        Returns
        -------
        Dataset
            Instantiated dataset populated with numpy arrays.

        Raises
        ------
        ValueError
            If the provided domain is not registered in the metadata registry.
        """
        try:
            metadata = self.METADATA
        except KeyError:
            raise ValueError(f"Unknown metadata domain: {domain}")

        return dataset_cls.from_rows(
            rows,
            variable_scale_factor=metadata,
        )

    def build_ocean_data(self, *datasets):
        """
        Bundle one or more Dataset objects into a single OceanData container.

        This method enforces:
        - Unique dataset names within the container
        - A consistent return type for all query methods

        Parameters
        ----------
        *datasets : Dataset
            One or more Dataset instances to include in the container.

        Returns
        -------
        OceanData
            Container object holding all provided datasets.
        """
        ocean_data = OceanData()
        for ds in datasets:
            ocean_data.add(ds)
        return ocean_data