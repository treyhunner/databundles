'''
Created on Jul 14, 2012

@author: eric
'''
import unittest
import os.path

class Test(unittest.TestCase):


    def setUp(self):
        import yaml

        bundle_yaml = '''
identity:
    creator: creator
    dataset: dataset
    revision: 1
    source: source
    subset: subset
    variation: variation'''
     
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        cf = os.path.join(self.bundle_dir,'bundle.yaml')
      
        yaml.dump(yaml.load(bundle_yaml), file(cf, 'w'), indent=4, default_flow_style=False)

    def tearDown(self):
        pass


    def test_file_config(self):
        from databundles.bundle import BundleFileConfig
        
        bfc = BundleFileConfig(self.bundle_dir)
        
        
        self.assertEquals('subset',bfc.identity.subset)
        self.assertEquals('source',bfc.identity.source)
        
        self.assertEquals(bfc.identity.id,bfc.identity.id_ )
       
        bfc.identity.subset = 'foo'
       
        self.assertEquals('foo',bfc.identity.subset)
    
        bfc.rewrite()
        bfc = BundleFileConfig(self.bundle_dir)
        self.assertEquals('foo',bfc.identity.subset)
        
    def test_db_config(self):
        from databundles.bundle import BuildBundle, DbBundle
        
        b = BuildBundle(self.bundle_dir)
        b.clean() 
        b.database.create()
        
        db_path =  b.database.path
        
        dbb = DbBundle(db_path)
        
        config = dbb.config #@UnusedVariable
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()