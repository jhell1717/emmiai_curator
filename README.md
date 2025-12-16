### EmmiAI ETL Pipeline

#### Objectives:
* Convert EmmiAI .pt data into zarr format ready for training NVIDIA PhysicsNeMo DoMINO Deep Learning Model.

### To Run:
```python run_etl.py --config-name config etl.source.input_dir=test_pc_domino/ etl.sink.output_dir=output_zarr/```