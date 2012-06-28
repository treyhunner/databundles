'''
Created on Jun 22, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle

class Test(unittest.TestCase):

    def setUp(self):
        
        import os.path, yaml
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        db_file = os.path.join(self.bundle_dir,'build/bundle.db')
        if os.path.exists(db_file):
            os.remove(db_file)
        
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
     
        cf = os.path.join(self.bundle_dir,'bundle.yaml')
      
        yaml.dump(yaml.load(bundle_yaml), file(cf, 'w'), indent=4, default_flow_style=False)
     
     
        self.bundle = Bundle(self.bundle_dir)
      
    def test_new_bundle(self):
        pass # Just create the bundle. 
        
    def test_bundle_init(self):
        
        import yaml, time
        
        cd = self.bundle.config.config_dict
        cf = self.bundle.config.config_file
     
        self.assertIn('id', cd['identity'])
        self.assertEquals('clarinova.com', self.bundle.identity.creator)
        oid = self.bundle.identity.id_
    
        time.sleep(1)
       
        # Test that the build.yaml file is reloaded, but that the
        # id value does not change. 
        cd['identity']['creator'] = 'foobar'
        yaml.dump(cd, file(cf, 'w'), indent=4, default_flow_style=False)

        bundle =  Bundle(self.bundle_dir)     
        self.assertEquals('foobar', bundle.identity.creator)
        self.assertEquals(oid, bundle.identity.id_)

      
    def test_identity(self):
        self.assertEqual('census.gov', self.bundle.identity.source)
        self.assertEqual('2000 Population Census', self.bundle.identity.dataset)
        self.assertEqual('SF1', self.bundle.identity.subset)
        self.assertEqual('orig', self.bundle.identity.variation)
        self.assertEqual('clarinova.com', self.bundle.identity.creator)
        self.assertEqual(1, int(self.bundle.identity.revision))
        self.assertEqual('census.gov-2000_population_census-sf1-orig-a7d9-r1', 
                         self.bundle.identity.name)
      
        self.bundle.identity.source = 'foobar'
        
        self.assertEqual('foobar-2000_population_census-sf1-orig-a7d9-r1', 
                         self.bundle.identity.name)
     
    def test_schema_direct(self):
        '''Test adding tables directly to the schema'''
        s = self.bundle.schema
        s.add_table('table 1', altname='alt name a')
        s.add_table('table 2', altname='alt name b')
        s.add_table('table 1', altname='alt name c')
        
        self.bundle.identity.oid = 'foobar'
        
        # Test that schema id changes propagate. 
        for table in self.bundle.schema.tables:
            self.assertEqual('foobar',table.d_id)
            print table.oid, table.name, table.altname
        
        t = s.add_table('table 3', altname='alt name')
        
        t.add_column('col 1',altname='altname1')
        t.add_column('col 2',altname='altname2')
        t.add_column('col 3',altname='altname3')
        t.add_column('col 3',altname='altname3')
        
        for column in t.columns:
            print column.id_, column.name
     
        
    def test_generate_schema(self):
        self.bundle.schema.generate()
        
        
        
    def test_data(self):
        ds = self.bundle.config.get_or_new_dataset()
        s = self.bundle.database.session
    
        ds.data['foo'] = 'bat'
        
        print ds.data['foo']
        
        s.commit()

if __name__ == "__main__":
    if True:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        suite.addTest(Test('test_bundle_init'))
        #unittest.TextTestRunner().run(suite)