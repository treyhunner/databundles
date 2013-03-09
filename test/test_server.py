'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
import os.path
import logging 
import databundles.util
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig
from test_base import  TestBase
from  databundles.client.rest import Rest #@UnresolvedImport
from databundles.library import QueryCommand, get_library

server_url = 'http://localhost:7979'

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 
logging.captureWarnings(True)

class Test(TestBase):
 
    def setUp(self):

        self.copy_or_build_bundle()
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')    
        self.rc = RunConfig([os.path.join(self.bundle_dir,'client-test-config.yaml'),
                             os.path.join(self.bundle_dir,'bundle.yaml')])
           
        self.bundle = Bundle()  
        self.bundle_dir = self.bundle.bundle_dir
        self.start_server(server_url)

    def tearDown(self):
        self.stop_server(server_url)

    def get_library(self, name='default'):
        """Clear out the database before the test run"""

        return get_library(self.rc, name=name, reset = True)
    
    def test_simple_install(self):
        from databundles.library import QueryCommand
          
        api = Rest(server_url)

        r =  api.put(self.bundle.identity.id_, self.bundle.database.path)
      
        self.assertEquals(self.bundle.identity.name,r.object.get('name',''))
  
        r = api.get(self.bundle.identity.name, file_path = True )

        self.assertTrue(os.path.exists(r))
        os.remove(r)


        for partition in self.bundle.partitions:
            r =  api.put(partition.identity.id_,partition.database.path)
            self.assertEquals(partition.identity.name,r.object.get('name',''))
            
            r = api.get(partition.identity.name, file_path = True )

            self.assertTrue(os.path.exists(r))
            os.remove(r)
  
        # Try variants of find. 
        r = api.find(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r[0].Dataset.name)
        
        r = api.find(QueryCommand().identity(name = self.bundle.identity.name))
        self.assertEquals(self.bundle.identity.name, r[0].Dataset.name)

        for partition in self.bundle.partitions:
            r = api.find((QueryCommand().partition(name = partition.identity.name)).to_dict())
            self.assertEquals(partition.identity.name, r[0].Partition.name)
  
    def test_remote_library(self):
   
        # This test does not work with the threaded test server. 
        
        # It does work with an external server, but you have to delete 
        # All of the files on the remote library between runs. 
   
        #
        # First store the files in the local library
        #
        
        self.get_library('clean').purge()
        self.get_library('server').purge()
        
        l = self.get_library()
     
        r = l.put(self.bundle)

        r = l.get(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r.bundle.identity.name)

        for partition in self.bundle.partitions:
            r = l.put(partition)

            # Get the partition with a name
            r = l.get(partition.identity.name)
            self.assertTrue(r is not False)
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.bundle.identity.name)
            
            # Get the partition with an id
            r = l.get(partition.identity.id_)
            self.assertTrue(r is not False)
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.bundle.identity.name)            

        #
        # Now start with a different, clean library with the same remote
        #

        # haven't pushed yet, so should fail. 
        l2 = self.get_library('clean')
        self.assertRaises(Exception, l2.get, self.bundle.identity.name)     
     
        # Copy all of the newly added files to the server. 
        l.push()
   
        l2 = self.get_library('clean')
        r = l2.get(self.bundle.identity.name)
        self.assertTrue(r is not False)

   
    def test_remote_library_partitions(self):
        

        l = self.get_library()
     
        r = l.put(self.bundle)

        r = l.get(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r.bundle.identity.name)

        for partition in self.bundle.partitions:
            r = l.put(partition)

            # Get the partition with a name
            r = l.get(partition.identity.name)
            self.assertTrue(r is not False)
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.bundle.identity.name)

        # Copy all of the newly added files to the server. 
        l.push()
            
        l2 = get_library('clean')
        l2.purge()
        
        r = l2.get('b1DxuZ001')
        print r
        
        self.assertTrue(os.path.exists(r.partition.database.path))
        
   
    def test_test(self):
        from databundles.client.siesta import  API
        a = API(server_url)
        
        # Test echo for get. 
        r = a.test.echo('foobar').get(bar='baz')
        
        self.assertEquals(200,r.status)
        self.assertIsNone(r.exception)
        
        self.assertEquals('foobar',r.object[0])
        self.assertEquals('baz',r.object[1]['bar'])
        
        # Test echo for put. 
        r = a.test.echo().put(['foobar'],bar='baz')
        
        self.assertEquals(200,r.status)
        self.assertIsNone(r.exception)

        self.assertEquals('foobar',r.object[0][0])
        self.assertEquals('baz',r.object[1]['bar'])
      
        
        with self.assertRaises(Exception):
            r = a.test.exception.put('foo')
        
        with self.assertRaises(Exception):
            r = a.test.exception.get()

    def test_put_bundle(self):
        from databundles.bundle import DbBundle
        from databundles.library import QueryCommand
        
        r = Rest(server_url)
        
        bf = self.bundle.database.path

        # With an FLO
        response =  r.put(self.bundle.identity.id_, open(bf))
        self.assertEquals(self.bundle.identity.id_, response.object.get('id'))
      
        # with a path
        response =  r.put(self.bundle.identity.id_, bf)
        self.assertEquals(self.bundle.identity.id_, response.object.get('id'))

        for p in self.bundle.partitions.all:
            response =  r.put(p.identity.id_, open(p.database.path))
            self.assertEquals(p.identity.id_, response.object.get('id'))

        # Now get the bundles
        bundle_file = r.get(self.bundle.identity.id_,'/tmp/foo')

        bundle = DbBundle(bundle_file)

        self.assertIsNot(bundle, None)
        self.assertEquals('a1DxuZ',bundle.identity.id_)

        # Should show up in datasets list. 
        
        o = r.list()
   
        self.assertTrue('a1DxuZ' in o.keys() )
    
        o = r.find(QueryCommand().table(name='tone').partition(any=True))
      
        self.assertTrue( 'b1DxuZ001' in [i.Partition.id for i in o])
        self.assertTrue( 'a1DxuZ' in [i.Dataset.id for i in o])
      
    def test_put_errors(self):
        pass

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())