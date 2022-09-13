To run default experiment, have a unique id for the machine and the workload 

```python3 Experiment.py *experiment ID* *machine ID* *workload ID*```

If AWS credentials have not been loaded into the system, you would need to pass it to the scirpt. 

```python3 Experiment.py *experiment ID* *machine ID* *workload ID* --awsKey XXX --awsSecret XXX```