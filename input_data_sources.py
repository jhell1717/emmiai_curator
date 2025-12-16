
from pathlib import Path
from typing import Any, Dict, List


import pyvista as pv

import glob
import os
import vtk  # noqa: F811
import numpy as np  # noqa: F811

from physicsnemo_curator.etl.data_sources import DataSource
from physicsnemo_curator.etl.processing_config import ProcessingConfig

class stlData(DataSource):
    def __init__(self,cfg:ProcessingConfig,input_dir:str):
        super().__init__(cfg)
        self.input_dir = Path(input_dir)

        if not self.input_dir.exists():
            raise FileNotFoundError(f('Input directory {self.input_dir} does not exist'))
        
    def get_file_list(self) -> List[str]:
        stl_files = []
        surface_file = []

        subdirs = [p for p in self.input_dir.iterdir() if p.is_dir()]

        for i in subdirs:
            stl_file = list(i.glob("*.stl"))
            surface_file = list(i.glob("*.vtp"))
            stl_files.append(stl_file)
            surface_file.append(surface_file)

        filenames = [f.stem for f in subdirs]

        self.logger.info(f'Found {len(filenames)} to process')
        return sorted(filenames)
    
    def read_file(self,filename:str) -> Dict[str,Any]:
        filepath = self.input_dir / f"{filename}/surface_{filename}_vtp.vtp"

        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        self.logger.warning(f"Reading {filepath}")

        data = {}

        mesh = pv.read(filepath)

        point_arrays = {k:v for k,v in mesh.point_data.items()}

        data['pressure'] = np.array(point_arrays['pressure'])
        data['coordinates'] = np.array(mesh.points)

        data['filename'] = filename

        self.logger.warning(f"Loaded data with {len(data['pressure'])} points")
        return data
    
    def _get_output_path(self,filename:str) -> Path:
        return NotImplementedError("DataSource only supports reading.")
    
    def _write_impl_temp_file(
            self,
            data:Dict[str,Any],
            output_path: Path) -> None:
        raise NotImplementedError('Data Source only Supports Reading')
    
    def should_skip(self,filename:str) -> bool:
        return False
    








