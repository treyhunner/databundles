'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
from sqlalchemy import * #@UnusedWildImport
from databundles.run import  RunConfig
from databundles.library import QueryCommand, get_library
import logging
import databundles.util

from test_base import  TestBase

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 

class Test(TestBase):
 
    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir

        self.root_dir = '/tmp/test_library'
        self.rc = RunConfig(os.path.join(self.bundle_dir,'bundle.yaml'))
        
        #databundles.util.get_logger('test_base').setLevel(logging.DEBUG) 
        #databundles.util.get_logger('filesystem').setLevel(logging.DEBUG)  
              
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
      
        return  get_library(self.rc)
        
    def tearDown(self):
        pass

    def delete(self):
        pass


    def test_resolve(self):
        """Test the resolve_id() function"""
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

    def foo(self, **kwargs):
        print "FOO", kwargs

    def test_library_install(self):
        '''Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent'''
      
        l = self.get_library()
     
        l.put(self.bundle)
        l.put(self.bundle)
        
        r = l.get(self.bundle.identity)

        self.assertIsNotNone(r.bundle)
        self.assertTrue(r.bundle is not False)
        self.assertEquals(self.bundle.identity.id_, r.bundle.identity.id_)
        
        print "Stored: ",  r.bundle.identity.name
        
        # Install the partition, then check that we can fetch it
        # a few different ways. 
        for partition in self.bundle.partitions:
            l.put(partition)
            
            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals( partition.identity.id_, r.partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(partition.identity.id_, r.partition.identity.id_)
            
        # Re-install the bundle, then check that the partitions are still properly installed

        l.put(self.bundle)
        
        for partition in self.bundle.partitions.all:
       
            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals(r.partition.identity.id_, partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(r.partition.identity.id_, partition.identity.id_)
            
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
       
        bp = l.get(r.Dataset.identity.id_)
        
        self.assertTrue(os.path.exists(bp.bundle.database.path))
        
        # Put the bundle with remove to check that the partitions are reset
        
        l.remove(self.bundle)
        
        r = l.find(QueryCommand().table(name='tone').partition(any=True)).all()
        self.assertEquals(0, len(r))      
        
        l.put(self.bundle)
    
        r = l.find(QueryCommand().table(name='tone').partition(any=True)).all()
        self.assertEquals(1, len(r))
       
        for ds in l.datasets:
            self.assertIn(ds.identity.name, ['source-dataset-subset-variation-ca0d-r1'])

    def test_cache(self):
        from databundles.filesystem import  FsCache, FsLimitedCache
     
        root =  self.root_dir 
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
       
        l1 =  FsLimitedCache(l1_repo_dir, upstream=l2, maxsize=5)
      
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


        # Check that the right files got deleted
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many5')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many6')))
        
        # Fetch a file that was displaced, to check that it gets loaded back 
        # into the cache. 
        p = l1.get('many1')
        p = l1.get('many2')
        self.assertTrue(p is not None)
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many1')))  
        # Should have deleted many6
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many6')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many7')))
        
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

    def test_compression_cache(self):
        '''Test a two-level cache where the upstream compresses files '''
        from databundles.filesystem import  FsCache,FsCompressionCache
         
        root =  self.root_dir 
        try: Test.rm_rf(root)
        except: pass
      
        l1_repo_dir = os.path.join(root,'comp-repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(root,'comp-repo-l2')
        os.makedirs(l2_repo_dir)
        
        testfile = os.path.join(root,'testfile')
        
        with open(testfile,'w+') as f:
            for i in range(1024): #@UnusedVariable
                f.write('.'*1023)
                f.write('\n')

        # Create a cache with an upstream wrapped in compression
        l3 = FsCache(l2_repo_dir)
        l2 = FsCompressionCache(l3)
        l1 = FsCache(l1_repo_dir, upstream=l2)
      
        f1 = l1.put(testfile,'tf1')         
  
        self.assertTrue(os.path.exists(f1))  
        
        l1.remove('tf1')
        
        self.assertFalse(os.path.exists(f1))  
        
        f1 = l1.get('tf1')
        
        self.assertTrue(os.path.exists(f1))  
        
    def make_s3_cache(self, root, size=None):
        '''Build a three state cache, with a to plevel limited cache, a second level
        compressed cache, and a third level compressed S3  cache. '''
        
        from databundles.filesystem import  FsCache, FsLimitedCache, FsCompressionCache
  
  
        l1 = self.bundle.filesystem.get_cache('test')
        return (l1.cache_dir, l1)
  
        l1_repo_dir = os.path.join(root,'s3-repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(root,'s3-repo-l2')
        os.makedirs(l2_repo_dir)
    
   
        # Create a cache with an upstream wrapped in compression
   
        l4 = self.bundle.filesystem.get_cache('s3')
        l3 = FsCache(l2_repo_dir, upstream=l4)
        l2 = FsCompressionCache(l3)
        
        if size is None:
            l1 = FsCache(l1_repo_dir, upstream=l2)
        else:
            l1 = FsLimitedCache(l1_repo_dir, upstream=l2, maxsize=size)
        
    
        return (l1_repo_dir, l1)
        
    def test_s3(self):

      
        #databundles.util.get_logger('databundles.filesystem').setLevel(logging.DEBUG) 
  
        # Set up the test directory and make some test files. 

        root =  self.root_dir 
        try: Test.rm_rf(root)
        except: pass
        os.makedirs(root)
                

        testfile = os.path.join(root,'testfile')
        
        with open(testfile,'w+') as f:
            for i in range(1024):
                f.write('.'*1023)
                f.write('\n')
         
        #fs = self.bundle.filesystem
        #local = fs.get_cache('downloads')
        repo_dir,local = self.make_s3_cache(root, 5)
        
        logger.info("repo_dir: {}".format(repo_dir))
      
        for i in range(0,10):
            logger.info("Putting "+str(i))
            local.put(testfile,'many'+str(i))
        
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many2')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))
                
        p = local.get('many1')
        self.assertTrue(p is not None)
                
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many2')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))
        
        p = local.get('many2')
        self.assertTrue(p is not None)
                
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))      
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many7'))) 
 
        p = local.get('many3')
        self.assertTrue(p is not None)
                
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many3')))      
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many7'))) 
 

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())