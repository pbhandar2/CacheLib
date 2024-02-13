""" This class contains the configurations needed to run 
    a block replay experiment. 
"""
import json 
import socket 
from pathlib import Path 

class ReplayConfig:
    def __init__(self, traces, backingFiles, t1_size_mb, **kwargs):
        # setup cache configuration 
        self.cache_config = {}
        self.cache_config["lruUpdateOnWrite"] = True 
        self.cache_config["cacheSizeMB"] = t1_size_mb
        self.cache_config["allocSizes"] = [4136]

        if "nvmCacheSizeMB" in kwargs:
            assert "nvmCachePaths" in kwargs 
            self.cache_config["nvmCacheSizeMB"] = kwargs["nvmCacheSizeMB"]
            self.cache_config["nvmCachePaths"] = kwargs["nvmCachePaths"]
            self.cache_config["navySizeClasses"] = []
            self.cache_config["navyBigHashSizePct"] = 0
            self.cache_config["truncateItemToOriginalAllocSizeInNvm"] = True
            self.cache_config["printNvmCounters"] = True 

            if "navyCleanRegions" in kwargs:
                self.cache_config["navyCleanRegions"] = kwargs["navyCleanRegions"]
            
            if "navyRegionSizeMB" in kwargs:
                self.cache_config["navyRegionSizeMB"] = kwargs["navyRegionSizeMB"]
            
            if "navyAdmissionProbability" in kwargs:
                self.cache_config["navyAdmissionProbability"] = kwargs["navyAdmissionProbability"]
            
            if "navyMaxDeviceWriteRateMB" in kwargs:
                self.cache_config["navyMaxDeviceWriteRateMB"] = kwargs["navyMaxDeviceWriteRateMB"]
            
        # setup stress test configuration 
        self.test_config = {
            'name': 'block-storage',
            'generator': 'block-replay',
            'numThreads': len(traces),
            'blockReplayConfig': {}
        }

        if "statOutputDir" in kwargs:
            assert Path(kwargs["statOutputDir"]).exists()
            self.test_config['blockReplayConfig']["statOutputDir"] = kwargs["statOutputDir"]

        self.test_config['blockReplayConfig']['traces'] = traces 
        self.test_config['blockReplayConfig']['backingFiles'] = backingFiles 
        
        self.test_config['blockReplayConfig']['globalClock'] = False 
        if 'globalClock' in kwargs:
            self.test_config['blockReplayConfig']['globalClock'] = kwargs['globalClock']
        
        self.test_config['blockReplayConfig']['blockRequestProcesserThreads'] = 16
        if 'blockRequestProcesserThreads' in kwargs:
            self.test_config['blockReplayConfig']['blockRequestProcesserThreads'] = kwargs['blockRequestProcesserThreads']

        self.test_config['blockReplayConfig']['asyncIOReturnTrackerThreads'] = 16
        if 'asyncIOReturnTrackerThreads' in kwargs:
            self.test_config['blockReplayConfig']['asyncIOReturnTrackerThreads'] = kwargs['asyncIOReturnTrackerThreads']
        
        self.test_config['blockReplayConfig']['minIatUs']  = 5
        if 'minIatUs' in kwargs:
            self.test_config['blockReplayConfig']['minIatUs'] = kwargs['minIatUs']
        
        self.test_config['blockReplayConfig']['maxPendingBlockRequestCount'] = 128 
        if 'maxPendingBlockRequestCount' in kwargs:
            self.test_config['blockReplayConfig']['maxPendingBlockRequestCount'] = kwargs['maxPendingBlockRequestCount']
        
        self.test_config['blockReplayConfig']['maxPendingBackingStoreIoCount'] = 64000
        if 'maxPendingBackingStoreIoCount' in kwargs:
            self.test_config['blockReplayConfig']['maxPendingBackingStoreIoCount'] = kwargs['maxPendingBackingStoreIoCount']
        
        self.test_config['blockReplayConfig']['minOffset'] = 0
        if 'minOffset' in kwargs:
            self.test_config['blockReplayConfig']['minOffset'] = kwargs['minOffset']

        self.test_config['blockReplayConfig']['replayRate'] = 1
        if 'replayRate' in kwargs:
            self.test_config['blockReplayConfig']['replayRate'] = kwargs['replayRate']
        
        self.test_config['blockReplayConfig']['waitAfterEmptyQueueUs'] = 10000
        if 'waitAfterEmptyQueueUs' in kwargs:
            self.test_config['blockReplayConfig']['waitAfterEmptyQueueUs'] = kwargs['waitAfterEmptyQueueUs']

        self.test_config['tag'] = socket.gethostname()
        if 'iteration' in kwargs:
            self.test_config['blockReplayConfig']['iteration'] = kwargs['iteration']
    

    def get_config(self):
        """ Get the config JSON. 

            Parameters
            ----------
            config : dict
                the cachebench config 
        """
        return {
            'cache_config': self.cache_config,
            'test_config': self.test_config
        }
        
    
    def generate_config_file(self, config_file_path):
        """ Generate a JSON configuration file based on the attributes of this 
            class. 

            Parameters
            ----------
            config_file_path : pathlib.Path/str 
                path to the configuration file 
        """
        with config_file_path.open('w+') as f:
            f.write(json.dumps(self.get_config(), indent=4))