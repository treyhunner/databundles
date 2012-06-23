'''
Created on Jun 22, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle

class Test(unittest.TestCase):

    def setUp(self):
        
        import os
        bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        bundle = Bundle(bundle_dir)     
        bundle.database.delete()
     
        self.bundle = Bundle(bundle_dir)
      
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

    def test_tables(self):
        s = self.bundle.schema
        s.add_table('table 1', altname='alt name1')
        s.add_table('table 2', altname='alt name2')
        s.add_table('table 1', altname='alt name1')
        
        self.bundle.identity.oid = 'foobar'
        
        for table in self.bundle.schema.tables:
            self.assertEqual('foobar',table.d_id)
            print table.oid, table.name, table.altname
        
        t = s.add_table('table 3', altname='alt name')
        
        t.add_column('col 1',altname='altname1')
        t.add_column('col 2',altname='altname2')
        t.add_column('col 3',altname='altname3')
        t.add_column('col 3',altname='altname3')
        
        for column in t.columns:
            print column.oid, column.name
        
    def test_bundle_init(self):
        
        print self.bundle.config.group('identity')
       
        
        

if __name__ == "__main__":
    if False:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        suite.addTest(Test('test_bundle_init'))
        #unittest.TextTestRunner().run(suite)