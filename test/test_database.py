'''
Created on Jul 16, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle

class Test(unittest.TestCase):

    def setUp(self):
    
        import os.path, yaml
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        Bundle(self.bundle_dir).clean()
     
        bundle_yaml = '''
identity:
    creator: creator
    dataset: dataset
    revision: 1
    source: source
    subset: subset
    variation: variation'''
     
        cf = os.path.join(self.bundle_dir,'bundle.yaml')
      
        yaml.dump(yaml.load(bundle_yaml), file(cf, 'w'), indent=4, default_flow_style=False)
      
        self.bundle = Bundle(self.bundle_dir)

    def tearDown(self):
        pass


    def test_basic(self):
        from databundles.library import LocalLibrary
        l = LocalLibrary()
        
        db = l.database
        
        db.create()

        db.clean()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()