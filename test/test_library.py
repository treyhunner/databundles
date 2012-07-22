'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
import databundles.library
from sqlalchemy import *

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
 
                   
                   
    def test_splitgeo(self):
        pass
        
    def x_test_BuildCombinedFile(self):

        import os.path
        import subprocess
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
          
    def x_test_basic(self):
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