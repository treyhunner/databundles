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

class Test(TestBase):
 
    def start_server(self):
        '''Run the Bottle server as a thread'''
        from databundles.client.siesta import  API
        import databundles.server.main
        from threading import Thread
        import time
        from functools import  partial
        
        logger.info("Starting library server")
        # Give the server a new RunCOnfig, so we can use a different library. 
        rc = RunConfig(os.path.join(self.bundle_dir,'server-test-config.yaml'),  load_all_paths = False)
        server = Thread(target = partial(databundles.server.main.test_run, rc) )
   
        server.setDaemon(True)
        server.start()
        
        #databundles.server.bottle.debug()

        #
        # Wait until the server responds to requests
        #
        a = API(server_url)
        for i in range(1,10): #@UnusedVariable
            try:
                # An echo request to see if the server is running. 
                a.test.echo('foobar').get(bar='baz') 
                break
            except:
                logger.info( 'Server not started yet, waiting')
                time.sleep(1)
                               
    def setUp(self):

        self.copy_or_build_bundle()
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')    
        
        self.bundle = Bundle()  
        self.bundle_dir = self.bundle.bundle_dir
        self.start_server()

    def get_library(self):
        """Clear out the database before the test run"""
        
        rc = RunConfig(os.path.join(self.bundle_dir,'client-test-config.yaml'),  load_all_paths = False)
        l = get_library(rc, 'client')
        
    
        l.database.clean()
        
        l.logger.setLevel(logging.DEBUG) 
        
        return l
        

    def tearDown(self):
        '''Shutdown the server process by calling the close() API, then waiting for it
        to stop serving requests '''
        
        from databundles.client.siesta import  API
        import time
        
        # Wait for the server to shutdown
        a = API(server_url)
        for i in range(1,10): #@UnusedVariable
            try:
                a.test.close.get()
                #print 'Teardown: server still running, waiting'
                time.sleep(1)
            except:
                break

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
        
        l.remove(self.bundle)
        print "Removed"
        r = l.get(self.bundle.identity)
        #self.assertFalse(r)
        
        #
        # Same story, but push to remote first, so that the removed
        # bundle will get loaded back rom the remote
        #
      
        l.put(self.bundle)
        l.push()
        r = l.get(self.bundle.identity)
        self.assertIsNotNone(r.bundle)
        l.remove(self.bundle)
        
        r = l.get(self.bundle.identity)
        self.assertIsNotNone(r.bundle)
        self.assertTrue(r.bundle is not False)
        self.assertEquals(self.bundle.identity.id_, r.bundle.identity.id_)
        

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())