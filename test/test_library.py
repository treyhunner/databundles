'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
import databundles.library
from sqlalchemy import *
from databundles.run import  RunConfig

class Test(unittest.TestCase):


    def setUp(self):
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.clean()
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.prepare()
        self.bundle.build()
        
    def get_library(self):
        
        cfg = self.bundle.config
        rc = RunConfig()
        rc.overlay(cfg.dict)
        return  databundles.library.get_library(rc)
        
    def tearDown(self):
        pass


    def testName(self):
        pass

    def test_resolve(self):
        from databundles import resolve_id
        
        self.assertEquals(self.bundle.identity.id_, resolve_id(self.bundle) )
        self.assertEquals(self.bundle.identity.id_, resolve_id(self.bundle.identity))
        self.assertEquals(self.bundle.identity.id_, resolve_id(self.bundle.identity.id_))
        self.assertEquals(self.bundle.identity.id_, resolve_id(str(self.bundle.identity.id_)))

        for partition in self.bundle.partitions.all:
            self.assertEquals(partition.identity.id_, resolve_id(partition))
            self.assertEquals(partition.identity.id_, resolve_id(partition.identity))
            self.assertEquals(partition.identity.id_, resolve_id(partition.identity.id_))
            self.assertEquals(partition.identity.id_, resolve_id(str(partition.identity.id_)))

    def test_library_install(self):
        '''Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent'''
        
        l = self.get_library()
     
        l.put(self.bundle)
        l.put(self.bundle)
        
        bundle = l.get(self.bundle.identity)
        
        self.assertIsNotNone(bundle)
        self.assertEquals(self.bundle.identity.id_, bundle.identity.id_)
        
        print bundle.identity.name
        
        # Install the partition, then check that we can fetch it
        # a few different ways. 
        for partition in self.bundle.partitions.all:
            l.put(partition)
            
            p2 = l.get(partition.identity)
            self.assertIsNotNone(p2)
            self.assertEquals(p2.identity.id_, partition.identity.id_)
            
            p2 = l.get(partition.identity.id_)
            self.assertIsNotNone(p2)
            self.assertEquals(p2.identity.id_, partition.identity.id_)
            
        # Re-install the bundle, then check that the partitions are still properly installed

        l.put(self.bundle, remove=False)
        
        for partition in self.bundle.partitions.all:
       
            p2 = l.get(partition.identity)
            self.assertIsNotNone(p2)
            self.assertEquals(p2.identity.id_, partition.identity.id_)
            
            p2 = l.get(partition.identity.id_)
            self.assertIsNotNone(p2)
            self.assertEquals(p2.identity.id_, partition.identity.id_)
            
        # Find the bundle and partitions in the library. 
    
        r = l.find(l.query().table(name='tone'))
        self.assertEquals('source-dataset-subset-variation-ca0d-r1',r[0].identity.name)  
    
        r = l.find(l.query().table(name='tone').partition(any=True)).all()
        self.assertEquals('source-dataset-subset-variation-ca0d-tone-r1',r[0].Partition.identity.name)
        
        r = l.find(l.query().table(name='tthree').partition(any=True)).all()
        self.assertEquals('source-dataset-subset-variation-ca0d-tthree-r1',r[0].Partition.identity.name)
        
        # Put the bundle with remove to check that the partitions are reset
        l.put(self.bundle, remove=True)
        
        r = l.find(l.query().table(name='tone'))
        self.assertEquals('source-dataset-subset-variation-ca0d-r1',r[0].identity.name)  
    
        r = l.find(l.query().table(name='tone').partition(any=True)).all()
        self.assertEquals(0, len(r))
        
        #
        # Rebuild from installed bundles. 
        
        l.rebuild()
        
        r = l.find(l.query().table(name='tone'))
        self.assertEquals('source-dataset-subset-variation-ca0d-r1',r[0].identity.name)  
    
        r = l.find(l.query().table(name='tone').partition(any=True)).all()
        self.assertEquals('source-dataset-subset-variation-ca0d-tone-r1',r[0].Partition.identity.name)
        
        r = l.find(l.query().table(name='tthree').partition(any=True)).all()
        self.assertEquals('source-dataset-subset-variation-ca0d-tthree-r1',r[0].Partition.identity.name)
        

    def x_test_basic(self):
        import sqlite3

        path = '/tmp/geo.db'
   
        t = Table('sf1geo', MetaData('sqlite:///'+path), autoload=True)
        db = create_engine('sqlite:///'+path)
        conn = db.connect()
        
        
        for c in t.columns:
            if str(c.type) == 'TEXT':
                rows = conn.execute(select([c]))
                
                i = 1000
                for row in rows:
                    i -= 1
                    if i == 0:
                        print "{}\tNUMBER".format(c.name)
                        break
                    
                    v = str(row[0]).strip()
                   
                    if len(v) == 0:
                        continue;
                    
                    try:
                        int(v)
                    except Exception as e:
                        print "{}\tTEXT".format(c.name)
                        break
 

    def x_test_BuildCombinedFile(self):


        import sqlite3
        
        l =  databundles.library.get_library()
          
        q = (l.query()
                 .identity(creator='clarinova.com', subset = 'sf1geo')
                 .partition(any=True)
            )

        path = '/tmp/geo.db'
        
        if os.path.exists(path):
            os.remove(path)
        
        geo = l.get(q.first.Partition)
      
        db = create_engine('sqlite:///'+path)
        Table('sf1geo', MetaData('sqlite:///'+geo.database.path), autoload=True).create(db)

        con = sqlite3.connect(path)

       
        for result in q.all:
            geo = l.get(result.Partition)
            print "GEO",geo.database.path     
     
            c = con.cursor()
            c.execute("""ATTACH DATABASE '{}' AS geo """.format(geo.database.path))
            c.execute("""INSERT INTO sf1geo SELECT * FROM geo.sf1geo""")
            c.execute("""DETACH DATABASE  geo """)
        
        c.close()
        
        return True 
          
    def xs_test_basic(self):
        import sqlite3
        import petl
         
        l =  databundles.library.get_library()
        
        rows = l.query().identity(creator='clarinova.com', subset = 'sf1').partition(any=True).all
        for r in rows:
           
            part = l.get(r.Partition)

            print "PART",part.database.path

            q = (l.query()
                 .identity(dataset = part.identity.dataset,source = part.identity.source,
                      creator = part.identity.creator, subset = 'sf1geo')
                 .partition(space = part.identity.space)
            )
            
            geo = l.get(q.one.Partition)
            
           
            print "GEO",geo.database.path
            
            for table in part.schema.tables:
                print part.identity.id_, part.name, table.name
                f = l.stream(part.identity.id_, table)
                g = l.stream(geo.identity.id_, 'select * from sf1geo') 
                
                con = sqlite3.connect(':memory:')
                c = con.cursor()
                c.execute("""ATTACH DATABASE '{}' AS part """.format(part.database.path))
                c.execute("""ATTACH DATABASE '{}' AS geo """.format(geo.database.path))
                
                q = """SELECT  p.*, g.*
                FROM part.{0} as p, geo.sf1geo as g
                WHERE p.logrecno = g.LOGRECNO AND g.SUMLEV = 80""".format(table.name)
                
                print q
                
                t = petl.fromdb(con, q)
                
                print petl.look(t)
                
                con.close()
                
                
                

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()