To run default experiment, have a unique ID to identify the experiment, workload and machine ready and run 

```python3 Experiment.py *experiment ID* *machine ID* *workload ID*```

The above code will generated a file out.csv in the home directory which is not pass the script to run. 

```./run_experiment_list.sh ~/out.csv```

For details in how to run specific experiments refer to the experiment sheet, 

https://docs.google.com/spreadsheets/d/1nuOhB131oT6aGcEzyawZ3A-ReagqyzdI0JizrLhV4KQ/edit?usp=sharing

Example command to run an experiment

```sudo python3 Experiment.py basic $MACHINE$ $WORKLOAD$; sudo ./run_experiment_list.sh ~/out.csv ```

