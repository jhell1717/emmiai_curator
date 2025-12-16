from pathlib import Path
from typing import Any, Dict, List

import zarr  # noqa: F811
from zarr.storage import LocalStore

from physicsnemo_curator.etl.data_sources import DataSource
from physicsnemo_curator.etl.processing_config import ProcessingConfig

class ZarrDataSource(DataSource):
    def __init__(self,cfg:ProcessingConfig,output_dir=str):
        super().__init__(cfg)
        self.output_dir = Path(output_dir)

        self.output_dir.mkdir(parents=True,exist_ok=True)

    def get_file_list(self) -> List[str]:
        raise NotImplementedError("ZarrDataSource only supports writing")

    def read_file(self, filename: str) -> Dict[str, Any]:
        """Not implemented - this DataSource only writes."""
        raise NotImplementedError("ZarrDataSource only supports writing")
    
    def _get_output_path(self,filename:str) -> Path:
        return self.output_dir / f"{filename}.zarr"
    
    def _write_impl_temp_file(self,data:Dict[str,Any],output_path:Path) -> None:

        self.logger.info(f"Creating Zarr store: {output_path}")
        store = LocalStore(output_path)
        root = zarr.open_group(store=store, mode="w")

        for array_name, array_info in data.items():
            data_array = array_info.get("data", None)
            compressors = array_info.get("compressor") if array_info.get("compressor") else None
            chunks = array_info.get("chunks", None)
            if data_array is not None:
                # provide data only (do NOT pass dtype together with data)
                root.create_array(
                    name=array_name,
                    data=data_array,
                    chunks=chunks,
                    compressors=compressors,
                )
            else:
                # create empty array with dtype/shape if no data provided
                root.create_array(
                    name=array_name,
                    shape=array_info.get("shape"),
                    dtype=array_info.get("dtype"),
                    chunks=chunks,
                    compressors=compressors,
                )

        # Add some store-level metadata
        root.attrs["zarr_format"] = 3
        root.attrs["created_by"] = "Joshua Hellewell"

        self.logger.info("Successfully created Zarr store")

    def should_skip(self, filename: str) -> bool:
        """Check if we should skip writing this store.

        Args:
            filename: Base filename to check

        Returns:
            True if store should be skipped (already exists)
        """
        store_path = self.output_dir / f"{filename}.zarr"
        exists = store_path.exists()

        if exists:
            self.logger.info(f"Skipping {filename} - Zarr store already exists")
            return True

        return False