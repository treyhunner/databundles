'''
Created on Jun 22, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle

class Test(unittest.TestCase):

    def setUp(self):

        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
    
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.clean()
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.prepare()
        self.bundle.build()
      
    def test_identity(self):
        self.assertEqual('source', self.bundle.identity.source)
        self.assertEqual('dataset', self.bundle.identity.dataset)
        self.assertEqual('subset', self.bundle.identity.subset)
        self.assertEqual('variation', self.bundle.identity.variation)
        self.assertEqual('creator', self.bundle.identity.creator)
        self.assertEqual(1, int(self.bundle.identity.revision))
        self.assertEqual('source-dataset-subset-variation-ca0d-r1', 
                         self.bundle.identity.name)

    def test_db_bundle(self):
        
        from databundles.bundle import BuildBundle, DbBundle
        
        b = BuildBundle(self.bundle_dir)
        b.clean()
        
        self.assertTrue(b.identity.id_ is not None)
        self.assertEquals('source-dataset-subset-variation-ca0d-r1', b.identity.name)

        b.database.create()
        
        db_path =  b.database.path
        
        dbb = DbBundle(db_path)
        
        self.assertEqual("source-dataset-subset-variation-ca0d-r1", dbb.identity.name)
        self.assertEqual("source-dataset-subset-variation-ca0d-r1", dbb.config.identity.name)
        
    def test_schema_direct(self):
        '''Test adding tables directly to the schema'''
        
        # If we don't explicitly set the id_, it will change for every run. 
        self.bundle.config.identity.id_ = 'aTest'

    
        s = self.bundle.schema
        s.add_table('table 1', altname='alt name a')
        s.add_table('table 2', altname='alt name b')
        
        self.assertRaises(Exception,  s.add_table, ('table 1', ))
      
        self.assertIn('c1qSlv01', [t.id_ for t in self.bundle.schema.tables])
        self.assertIn('c1qSlv02', [t.id_ for t in self.bundle.schema.tables])
        self.assertNotIn('cTest03', [t.id_ for t in self.bundle.schema.tables])
        
        t = s.add_table('table 3', altname='alt name')
        
        s.add_column(t,'col 1',altname='altname1')
        s.add_column(t,'col 2',altname='altname2')
        s.add_column(t,'col 3',altname='altname3')
      
        self.bundle.database.session.commit()
        
        self.assertIn('d1qSlv0601', [c.id_ for c in t.columns])
        self.assertIn('d1qSlv0602', [c.id_ for c in t.columns])
        self.assertIn('d1qSlv0603', [c.id_ for c in t.columns])
        
    def test_generate_schema(self):
        '''Uses the generateSchema method in the bundle'''
        from databundles.orm import  Column
        
        s = self.bundle.schema
        
        s.clean()
        
        t1 = s.add_table('table1')
                
        s.add_column(t1,name='col1', datatype=Column.DATATYPE_REAL )
        s.add_column(t1,name='col2', datatype=Column.DATATYPE_INTEGER )
        s.add_column(t1,name='col3', datatype=Column.DATATYPE_TEXT )  
        
        t2 = s.add_table('table2')   
        s.add_column(t2,name='col1' )
        s.add_column(t2,name='col2' )
        s.add_column(t2,name='col3' )   

        t3 = s.add_table('table3') 
        s.add_column(t3,name='col1', datatype=Column.DATATYPE_REAL )
        s.add_column(t3,name='col2', datatype=Column.DATATYPE_INTEGER )
        s.add_column(t3,name='col3', datatype=Column.DATATYPE_TEXT )   

        self.bundle.database.session.commit()
     
    def test_column_processor(self):
        from databundles.orm import  Column
        from databundles.transform import BasicTransform, CensusTransform
        
        s = self.bundle.schema  
        s.clean()
        
        t = s.add_table('table3') 
        s.add_column(t,name='col1', datatype=Column.DATATYPE_INTEGER, default=-1, illegal_value = '999' )
        s.add_column(t,name='col2', datatype=Column.DATATYPE_TEXT )   
        s.add_column(t,name='col3', datatype=Column.DATATYPE_REAL )
        
        
        self.bundle.database.session.commit()
        
        c1 = t.column('col1')

        
        self.assertEquals(1, BasicTransform(c1)({'col1': ' 1 '}))
        
        with self.assertRaises(ValueError):
            print "PROCESSOR '{}'".format(CensusTransform(c1)({'col1': ' B '}))
        
        self.assertEquals(1, CensusTransform(c1)({'col1': ' 1 '}))
        self.assertEquals(-1, CensusTransform(c1)({'col1': ' 999 ' }))
        self.assertEquals(-3, CensusTransform(c1)({'col1': ' # '}))
        self.assertEquals(-2, CensusTransform(c1)({'col1': ' ! '}))
       
        
    def test_partition(self):
        
 
        from  databundles.partition import  PartitionIdentity

        ## TODO THis does does not test the 'table' parameter of the ParitionId
          
        pid1 = PartitionIdentity(self.bundle.identity, time=1, space=1)

        
        pid2 = PartitionIdentity(self.bundle.identity, time=2, space=2)
        pid3 = PartitionIdentity(self.bundle.identity, space=3,)
        
        
        self.bundle.partitions.new_partition(pid1, data={'pid':'pid1'})
    
        self.bundle.partitions.new_partition(pid2, data={'pid':'pid2'})
        self.bundle.partitions.new_partition(pid3, data={'pid':'pid3'})
        self.bundle.partitions.new_partition(pid1, data={'pid':'pid1'})
        self.bundle.partitions.new_partition(pid2, data={'pid':'pid21'})
        self.bundle.partitions.new_partition(pid3, data={'pid':'pid31'})
        
        self.bundle.database.session.commit()
        
        # 3 partitions from the build, three we just created. 
        self.assertEqual(6, len(self.bundle.partitions.all))
        
        p = self.bundle.partitions.new_partition(pid1)   
        self.assertEquals('pid1',p.data['pid'] )
      
        p = self.bundle.partitions.new_partition(pid2)   
        self.assertEquals('pid2',p.data['pid'] ) 

        p = self.bundle.partitions.new_partition(pid3)   
        self.assertEquals('pid3',p.data['pid'] ) 

        p = self.bundle.partitions.find(pid1)   
        self.assertEquals('pid1',p.data['pid'] )
      
        p = self.bundle.partitions.find(pid2)   
        self.assertEquals('pid2',p.data['pid'] ) 

        p = self.bundle.partitions.find(pid3)   
        self.assertEquals('pid3',p.data['pid'] ) 
         
        p = self.bundle.partitions.find_orm(pid3)   
        s = self.bundle.database.session
        p.data['foo'] = 'bar'
        s.commit()
        
        p = self.bundle.partitions.find(pid3)   
        self.assertEquals('bar',p.data['foo'] ) 
       
        s.commit()
        p.database.create()
        
    def test_config(self):
        
        c = self.bundle.config
        
        db_config = self.bundle.db_config
       
        db_config.foo.bar = 'bingo'
       
        self.assertEquals('bingo', db_config.foo.bar)
     
        
    def x_test_tempfile(self):
  
        self.test_generate_schema()
  
        table = self.bundle.schema.tables[0]
        print "TABLE", table.name
        tf = self.bundle.database.tempfile(table)
        
        print "PATH",tf.path
        w = tf.writer
        
        for i in range(10):
            w.writerow([i,i,i])
        
        
        
if __name__ == "__main__":
    if True:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        suite.addTest(Test('test_bundle_init'))
        #unittest.TextTestRunner().run(suite)