
import os  
import sys 

import hydra
from hydra.utils import instantiate
from omegaconf import DictConfig

from physicsnemo_curator.etl.etl_orchestrator import ETLOrchestrator
from physicsnemo_curator.etl.processing_config import ProcessingConfig
from physicsnemo_curator.utils import utils as curator_utils

@hydra.main(version_base="1.3", config_path="conf")
def main(cfg: DictConfig) -> None:
    
    
    curator_utils.setup_multiprocessing() # JH : Prep for multiprocessing of files.

    processing_config = ProcessingConfig(**cfg.etl.processing) # JH: Specify processing params via config.yaml file.

    source = instantiate(cfg.etl.source, processing_config) # JH: Setup input directory of flowfield & geometry files.

    sink = instantiate(cfg.etl.sink, processing_config) # JH: Setup output location for zarr files.

    # JH: Setup transformation specified in config.yaml
    cfgs = {k: {"_args_": [processing_config]} for k in cfg.etl.transformations.keys()}
    transformations = instantiate(cfg.etl.transformations, **cfgs)

    # JH: Initalise input, output, transformations, processing & validation (if required).
    orchestrator = ETLOrchestrator(
        source=source,
        sink=sink,
        transformations=transformations,
        processing_config=processing_config,

    )

    orchestrator.run() # JH: Run ETL pipeline.

if __name__ == "__main__":
    main()