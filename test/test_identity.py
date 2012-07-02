'''
Created on Jul 1, 2012

@author: eric
'''
import unittest
import yaml
import os.path
from databundles.identity import Identity #@UnusedImport
from databundles.bundleconfig import BundleConfig, BundleConfigFile #@UnusedImport

class Test(unittest.TestCase):


    def setUp(self):
        
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        
        bundle_yaml = '''
build:
    headers: sf1headers.csv
    rootUrl: http://www2.census.gov/census_2000/datasets/Summary_File_1/
    statesFile: States
identity:
    creator: clarinova.com
    dataset: 2000 Population Census
    revision: 1
    source: census.gov
    subset: SF1
    variation: orig '''

        self.config = yaml.load(bundle_yaml)

    def test_bundle(self):
        
        bcf = BundleConfigFile(self.bundle_dir)
        
        print bcf.config_dict
        
        return True
         
        #id = Identity(**bcd.get('identity'))
              
        print id.name
        print id.path

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()