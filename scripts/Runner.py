"""Runner manages a process spawned using subprocess.Popen and periodically measures attributes
    of the machine during the lifetime of the process. The following is the description of the 
    example files created per process and its contents:
    - /dev/shm/cydonia-runner/usage.csv - Periodic CPU and memory usage of the machine during process lifetime. 
    - /dev/shm/cydonia-runner/power.csv - Power consumption of each CPU and memory device during process lifetime. 
    - /dev/shm/cydonia-runner/stdout.dump - Dump of stdout. 
    - /dev/shm/cydonia-runner/stderr.dump - Dump of stderr. 

    Attributes:
        cpu_memory_measurement_window_sec: Time period between snapshot of CPU and memory usage. 
        terminate_event: Event that triggers termination of thread that track CPU and memory usage. 
"""

from time import sleep 
from threading import Event
from subprocess import Popen 
from threading import Thread
from datetime import datetime 
from typing import List
from psutil import virtual_memory, cpu_percent
from pandas import read_csv, concat, DataFrame
from pyJoules.energy_meter import EnergyContext
from pyJoules.handler.csv_handler import CSVHandler


def track_memory_cpu_usage_thread_function(
    terminate_thread_event: Event, 
    measurement_window_sec: int, 
    output_path: str
) -> None:
    """Thread taking periodic snapshot of CPU and memory usage during the lifetime of the process. 

    Args:
        terminate_thread_event: Event object that is triggered to terminate the thread once the process has completed. 
        measurement_window_sec: Time period between snapshots of CPU and memory usage. 
        output_path: Path to file where snapshots of CPU and memory usage are stored. 
    """
    while True:
        sleep(measurement_window_sec)
        mem_stats = virtual_memory()
        cpu_stats = cpu_percent(percpu=True)

        out_dict = {}
        out_dict["ts"] = datetime.timestamp(datetime.now())
        out_dict["set"] = terminate_thread_event.is_set()
        for mem_stat_field in mem_stats._fields:
            mem_stat_value = getattr(mem_stats, mem_stat_field)
            out_dict["mem_{}".format(mem_stat_field)] = mem_stat_value
        for cpu_index, cpu_stat in enumerate(cpu_stats):
            out_dict["cpu_{}".format(cpu_index)] = cpu_stat

        df = DataFrame([out_dict])
        if output_path.exists() and output_path.stat().st_size > 0:
            old_df = read_csv(output_path)
            df = concat([old_df, df], ignore_index=True)
        df.to_csv(output_path, index=False)

        if terminate_thread_event.is_set():
            break 


class Runner:
    def __init__(
        self, 
        cpu_memory_measurement_window_sec: int = 60
    ) -> None:
        """Constructor for Runner class. 

            Args:
                cpu_memory_measurement_window_sec: Time period between snapshots of CPU and memory usage. 
        """
        self.cpu_memory_measurement_window_sec = cpu_memory_measurement_window_sec


    def run(
        self, 
        process_cmd: List[str], 
        stdout_path: str, 
        stderr_path: str, 
        cpu_memory_usage_path: str, 
        power_consumption_path: str
    ) -> int:
        """Spawn a process with a given command and its cpu, memory and power statistics. 

            Args:
                process_cmd: List of strings of the command to spawn a process. To spawn a process
                    using command "ls -lh", you would pass the equivalent of "ls -lh".split(" ") which 
                    is ["ls", "-lh"].
                stdout_path: Path to file that stores stdout.
                stderr_path: Path to file that stores stderr. 
                cpu_memory_usage_path: Path to file that stores periodic CPU and memory usage during process lifetime. 
                power_consumption_path Path to file that stores power consumption during process lifetime. 

            Returns:
                exit_code: The exit code of the process. 
        """
        exit_code = -1 

        # Start the thread that tracks CPU and memory usage.
        terminate_event = Event() 
        memory_cpu_usage_thread = Thread(
            target=track_memory_cpu_usage_thread_function, 
            args=(terminate_event, self.cpu_memory_measurement_window_sec, cpu_memory_usage_path))
        memory_cpu_usage_thread.start()

        # Start the process within the an energy context that tracks power consumption. 
        power_csv_handler = CSVHandler(power_consumption_path)
        with EnergyContext(handler=power_csv_handler) as energy_ctx, \
            open(stdout_path, "w+") as stdout_handle, \
                open(stderr_path, "w+") as stderr_handle:
        
            with Popen(process_cmd.split(' '), 
                stdout=stdout_handle, 
                stderr=stderr_handle
            ) as process_handle:
                process_handle.wait()
                exit_code = process_handle.returncode
        
        # Notify the thread tracking CPU and memory usage to terminate and wait. 
        terminate_event.set()
        memory_cpu_usage_thread.join()
        power_csv_handler.save_data()
        return exit_code