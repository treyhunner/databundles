'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig

from  databundles.client.rest import Rest

class Test(unittest.TestCase):


    def setUp(self):
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        
        self.rc = RunConfig(os.path.join(self.bundle_dir,'bundle.yaml'))
         
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.clean()
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.prepare()
        self.bundle.build()


    def tearDown(self):
        pass


    def test_basic(self):
        r = Rest('http://localhost:8080')
        
        resource, response =  r.test_put(open('/etc/hosts','r'))

        print resource.attrs
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()