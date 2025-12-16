from typing import Any, Dict

import numpy as np  # noqa: F811
import zarr

from physicsnemo_curator.etl.data_transformations import DataTransformation
from physicsnemo_curator.etl.processing_config import ProcessingConfig

class STLfileZARR(DataTransformation):
    def __init__(self,cfg:ProcessingConfig,chunk_size:int=10,compression_level: int = 3):
        super().__init__(cfg)
        self.compression_level = compression_level
        self.chunk_size = chunk_size

        self.compressor = zarr.codecs.BloscCodec(
            cname="zstd",
            clevel=self.compression_level,
            shuffle=zarr.codecs.BloscShuffle.shuffle,
        )

    def transform(self,data:Dict[str,Any]) -> Dict[str,Any]:
        self.logger.info(f"Transforming {data['filename']} for Zarr Storage")

        num_points = len(data['pressure'])

        chunk_points = min(self.chunk_size,num_points)

        zarr_data= {
            "pressure" : {},
            "coordinates" : {}
        }

        zarr_data['pressure'] = {
            "data": data['pressure'].astype(np.float32),
            "chunks":(chunk_points,),
            "compressor":self.compressor,
            "dtype":np.float32
        }

        zarr_data['coordinates'] = {
            "data" : data['coordinates'].astype(np.float32),
            "chunks" : (chunk_points,3),
            "compressor" : self.compressor,
            "dtype" : np.float32,
        }

        return zarr_data

