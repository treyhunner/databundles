'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig

from  databundles.client.rest import Rest #@UnresolvedImport

server_url = 'http://localhost:7979'

class Test(unittest.TestCase):



    def start_server(self):
        '''Run the Bottle server as a thread'''
        from databundles.client.siesta import  API
        import databundles.server.main
        from threading import Thread
        import time
        
        server = Thread(target = databundles.server.main.test_run)
        
        server.setDaemon(True)
        server.start()
        
        databundles.server.bottle.debug()

        a = API(server_url)
        for i in range(1,10): #@UnusedVariable
            try:
                a.test.echo('foobar').get(bar='baz')
                break
            except:
                print 'Server not started yet, waiting'
                time.sleep(1)
                               


    def setUp(self):
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        
        self.rc = RunConfig(os.path.join(self.bundle_dir,'bundle.yaml'))
         
        bundle = Bundle()     
        bundle.clean()
        
        self.bundle = Bundle()
        
        self.bundle.prepare()
        self.bundle.build()
        self.bundle_dir = self.bundle.bundle_dir

        self.start_server()

    def tearDown(self):
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
            r = a.test.exception.get()

                  

    def test_put_bundle(self):
        from databundles.bundle import DbBundle
        from databundles.library import QueryCommand
        
        r = Rest(server_url)
        
        bf = self.bundle.database.path
        
        # With an FLO
        response =  r.put(open(bf))
        self.assertEquals(self.bundle.identity.id_, response.object.get('dataset').get('id'))
      
        # with a path
        response =  r.put(bf)
        self.assertEquals(self.bundle.identity.id_, response.object.get('dataset').get('id'))
      
        # The bundle should have already installed the partitions, if they existsed, 
        # but it doesn't hurt to do it again. 
        for p in self.bundle.partitions.all:
            response =  r.put(open(p.database.path))
            self.assertEquals(p.identity.id_, response.object.get('partition').get('id'))

        # Now get the bundles
        bundle_file = r.get(self.bundle.identity.id_,'/tmp/foo')

        bundle = DbBundle(bundle_file)

        self.assertIsNot(bundle, None)
        self.assertEquals('a1DxuZ',bundle.identity.id_)

        # Should show up in datasets list. 
        o = r.datasets()
      
        
        self.assertTrue('a1DxuZ' in o.keys() )
    
        o = r.find(QueryCommand().table(name='tone').partition(any=True))
      
        self.assertTrue( 'b1DxuZ001' in [i.Partition.id_ for i in o])
        self.assertTrue( 'a1DxuZ' in [i.Dataset.id_ for i in o])
      

    def test_cache(self):
    
        import tempfile
        import uuid

        root = os.path.join(tempfile.gettempdir(),'testing',str(uuid.uuid4()))
        print "ROOT",root

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())