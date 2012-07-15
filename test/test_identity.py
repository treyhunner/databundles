'''
Created on Jul 1, 2012

@author: eric
'''
import unittest

from databundles.identity import Identity 
from databundles.partition import PartitionIdentity 
import copy

class Test(unittest.TestCase):
    

    def setUp(self):
        self.id_config = {
                        'creator': 'creator',
                        'dataset': 'dataset',
                        'revision': 1,
                        'source': 'source',
                        'subset': 'subset',
                        'variation': 'variation' 
                       }


        self.pid_config = copy.copy( self.id_config )
        
        self.pid_config['table'] = 'table'
        self.pid_config['space'] = 'space'
        self.pid_config['time'] = 'time'

    def test_bundle(self):
        id = Identity(**self.id_config); #@ReservedAssignment
      
        for k, v in self.id_config.items():
            self.assertEquals(v, getattr(id, k));
       
        self.assertEquals('source-dataset-subset-variation-ca0d-r1', id.name)
        
        pid = PartitionIdentity(**self.pid_config);
      
        for k, v in self.pid_config.items():
            self.assertEquals(v, getattr(pid, k));
       
        self.assertEquals('source-dataset-subset-variation-ca0d-time.space.table-r1', pid.name)
       
        pid = PartitionIdentity(id, time='time', space='space')
        
        self.assertEquals('source-dataset-subset-variation-ca0d-time.space-r1', pid.name)
       
        self.assertEquals('source/dataset-subset-variation-ca0d-r1/time/space', pid.path)
      

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()