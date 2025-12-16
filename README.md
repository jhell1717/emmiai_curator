### EmmiAI ETL Pipeline

#### Objectives:
* Convert EmmiAI .pt data into zarr format ready for training NVIDIA PhysicsNeMo DoMINO Deep Learning Model.

### To Run:
In the root of the curator directory, run the following:
```!python run_etl.py --config-name config etl.source.input_dir=test_pc_domino/ etl.sink.output_dir=output_zarr/```
* Specify the directory containing the CFD data.

### To Do:
* Incorporate volume and surface field data in the ```input_data_source``` module.
* Create branch for direct ETL from origina EmmiAI data format (i.e., from .pt tensors).