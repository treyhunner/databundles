'''
Created on Jun 19, 2012

@author: eric
'''
import unittest
from  databundles.bundle import Bundle


class Test(unittest.TestCase):


    def setUp(self):
        import os
        bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_files')
        self.bundle = Bundle(bundle_dir)
       


    def tearDown(self):
        pass


    def testName(self):
        import pprint
        
        pprint.pprint(self.bundle.identity)
        
        print self.bundle.name
        
        self.bundle.partition.time = 'today'
        self.bundle.partition.space = None
        
        print self.bundle.name
        
        self.bundle.partition.time = None
        self.bundle.partition.space = None
        self.bundle.partition.table = None
        
        print self.bundle.name
        
        import copy
        
        b = copy.deepcopy(self.bundle)
        b.partition.space = 'foobar'
        
        print b.name
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()