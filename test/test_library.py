'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle

class Test(unittest.TestCase):


    def setUp(self):
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.database.delete();
        self.bundle = Bundle(self.bundle_dir)
        


    def tearDown(self):
        pass


    def testName(self):
        pass


    def test_basic(self):
        self.bundle.prepare()
        self.bundle.build()
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()