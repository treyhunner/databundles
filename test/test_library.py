'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
from sqlalchemy import *
from databundles.run import  RunConfig
from databundles.library import QueryCommand, get_library

class Test(unittest.TestCase):

    def setUp(self):

        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
 
        self.rc = RunConfig(os.path.join(self.bundle_dir,'bundle.yaml'))
            
        try: self.rm_rf(self.rc.library.root)
        except: pass
        
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.clean()
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.prepare()
        self.bundle.build()
        
    @staticmethod
    def rm_rf(d):
        for path in (os.path.join(d,f) for f in os.listdir(d)):
            if os.path.isdir(path):
                Test.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)
        
    def get_library(self):
        
        ldb = self.rc.library.database['dbname']
        
        if os.path.exists(ldb):
            os.remove(ldb)
        
        cfg = self.bundle.config
        rc = RunConfig()
        rc.overlay(cfg.dict)
        return  get_library(rc)
        
    def tearDown(self):
        pass

    def delete(self):
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
        self.assertTrue(bundle is not False)
        self.assertEquals(self.bundle.identity.id_, bundle.identity.id_)
        
        print bundle.identity.name
        
        # Install the partition, then check that we can fetch it
        # a few different ways. 
        for partition in self.bundle.partitions.all:
            l.put(partition)
            
            p2 = l.get(partition.identity)
            self.assertIsNotNone(p2)
            self.assertEquals( partition.identity.id_, p2.identity.id_)
            
            p2 = l.get(partition.identity.id_)
            self.assertIsNotNone(p2)
            self.assertEquals(partition.identity.id_, p2.identity.id_)
            
        # Re-install the bundle, then check that the partitions are still properly installed

        l.put(self.bundle)
        
        for partition in self.bundle.partitions.all:
       
            p2 = l.get(partition.identity)
            self.assertIsNotNone(p2)
            self.assertEquals(p2.identity.id_, partition.identity.id_)
            
            p2 = l.get(partition.identity.id_)
            self.assertIsNotNone(p2)
            self.assertEquals(p2.identity.id_, partition.identity.id_)
            
        # Find the bundle and partitions in the library. 
    
        r = l.find(QueryCommand().table(name='tone'))
        self.assertEquals('source-dataset-subset-variation-ca0d-r1',r[0].Dataset.identity.name)  
    
        r = l.find(QueryCommand().table(name='tone').partition(any=True)).all()
        self.assertEquals('source-dataset-subset-variation-ca0d-tone-r1',r[0].Partition.identity.name)
        
        r = l.find(QueryCommand().table(name='tthree').partition(any=True)).all()
        self.assertEquals('source-dataset-subset-variation-ca0d-tthree-r1',r[0].Partition.identity.name)
        
        #
        #  Try getting the files 
        # 
        
        r = l.find(QueryCommand().table(name='tthree').partition(any=True)).one() #@UnusedVariable
       
        b = l.get(r.Dataset.identity.id_)
        
        self.assertTrue(os.path.exists(b.database.path))
        
        # Put the bundle with remove to check that the partitions are reset
        
        l.remove(self.bundle)
        l.put(self.bundle)
    
        r = l.find(QueryCommand().table(name='tone').partition(any=True)).all()
        self.assertEquals(0, len(r))
       
        for ds in l.datasets:
            self.assertIn(ds.identity.name, ['source-dataset-subset-variation-ca0d-r1'])

    def test_cache(self):
        from databundles.library import  FsCache, LibraryDbCache, NullCache
        import tempfile
        import uuid
        
        root = '/tmp/test_library'
        try: Test.rm_rf(root)
        except: pass
      
        l1_repo_dir = os.path.join(root,'repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(root,'repo-l2')
        os.makedirs(l2_repo_dir)
        
        testfile = os.path.join(root,'testfile')
        
        with open(testfile,'w+') as f:
            for i in range(1024):
                f.write('.'*1023)
                f.write('\n')
        
        #
        # Basic operations on a cache with no upstream
        #
        l2 =  FsCache(l2_repo_dir)

        p = l2.put(testfile,'tf1')
        l2.put(testfile,'tf2')
        g = l2.get('tf1')
                
        self.assertTrue(os.path.exists(p))  
        self.assertTrue(os.path.exists(g))
        self.assertEqual(p,g)

        self.assertIsNone(l2.get('foobar'))

        l2.remove('tf1')
        
        self.assertIsNone(l2.get('tf1'))
       
        #
        # Now create the cache with an upstream, the first
        # cache we created
       
        l1 =  FsCache(l1_repo_dir, upstream=l2, maxsize=5)
       
        g = l1.get('tf2')
        self.assertTrue(g is not None)
     
        # Put to one and check in the other. 
        
        l1.put(testfile,'write-through')
        self.assertIsNotNone(l2.get('write-through'))
             
        l1.remove('write-through', propagate=True)
        self.assertIsNone(l2.get('write-through'))

        # Put a bunch of files in, and check that
        # l2 gets all of the files, but the size of l1 says constrained
        for i in range(0,10):
            l1.put(testfile,'many'+str(i))
            
        self.assertEquals(4194304, l1.size)
        self.assertEquals(11534336, l2.size)

        # Check that the right files got deleted
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many6')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many7')))
        
        # Fetch a file that was displaced, to check that it gets loaded back 
        # into the cache. 
        p = l1.get('many1')
        p = l1.get('many2')
        self.assertTrue(p is not None)
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many1')))  
        # Should have deleted many7
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many7')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many8')))
        
        #
        # Check that verification works
        # 
        l1.verify()

        os.remove(os.path.join(l1.cache_dir, 'many8'))
            
        with self.assertRaises(Exception):                
            l1.verify()

        l1.remove('many8')
      
        l1.verify()
        
        c = l1.database.cursor()
        c.execute("DELETE FROM  files WHERE path = ?", ('many9',) )
        l1.database.commit()
        
        with self.assertRaises(Exception):        
            l1.verify()
        
        l1.remove('many9')
      
        l1.verify()
        
        # Test the LibraryDb Cache on  its own. 
#        l = self.get_library()
#        db = l.database
#        
#        ldc = LibraryDbCache(db, NullCache())
#        db.install_bundle(self.bundle)
#        
#        r = ldc.find(QueryCommand().table(name='tone'))
#        self.assertEquals('source-dataset-subset-variation-ca0d-r1',r[0].Dataset.identity.name)
#    
#        # Now, passthrough
#        ldc = LibraryDbCache(db, l2)
#        l1 =  FsCache(l1_repo_dir, upstream=ldc)
#        
#        g = l1.get('tf2')
#        self.assertTrue(g is not None)
#     
#        r = l1.find(QueryCommand().table(name='tone'))
#        self.assertEquals('source-dataset-subset-variation-ca0d-r1',r[0].Dataset.identity.name)
        

     
        
    def x_text_rebuild(self):
        #
        # Rebuild from installed bundles. 
        
        l = self.get_library()
        
        l.rebuild()
        
        r = l.find(l.query().table(name='tone'))
        self.assertEquals('source-dataset-subset-variation-ca0d-r1',r[0].identity.name)  
    
        r = l.find(l.query().table(name='tone').partition(any=True)).all()
        self.assertEquals('source-dataset-subset-variation-ca0d-tone-r1',r[0].Partition.identity.name)
        
        r = l.find(l.query().table(name='tthree').partition(any=True)).all()
        self.assertEquals('source-dataset-subset-variation-ca0d-tthree-r1',r[0].Partition.identity.name)
        

    def x_test_server(self):
        import uuid
        from  boto.s3.connection import S3Connection, Key
        
        access = self.rc.library.repository['access']
        secret = self.rc.library.repository['secret']
        bucket = self.rc.library.repository['bucket']
        
        conn = S3Connection(access, secret)

        for b in conn.get_all_buckets():
            print 'BUCKET', b.name
        
        
        b = conn.get_bucket(bucket)
        
        k = b.new_key('foo/bar')
        k.set_acl('public-read')
        
        k.set_contents_from_string(str(uuid.uuid4()))
        
        
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
        
        l =  get_library()
          
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
         
        l = get_library()
        
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