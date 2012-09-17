'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
import os.path
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig

from  databundles.client.rest import Rest #@UnresolvedImport

server_url = 'http://localhost:8080'
server_url = 'http://lorne:8080'

class Test(unittest.TestCase):


    def setUp(self):
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')
        
        self.rc = RunConfig(os.path.join(self.bundle_dir,'bundle.yaml'))
         
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.clean()
        self.bundle = Bundle(self.bundle_dir)
        
        self.bundle.prepare()
        self.bundle.build()


    def tearDown(self):
        pass


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
        r = Rest(server_url)
        
        bf = self.bundle.database.path
        response =  r.put(open(bf))
        self.assertEquals(self.bundle.identity.id_, response.object.get('dataset').get('id'))
      
        # The bundle should have already installed the partitions, if they existsed, 
        # but it doesn't hurt to do it again. 
        for p in self.bundle.partitions.all:
            response =  r.put(open(p.database.path))
            self.assertEquals(p.identity.id_, response.object.get('partition').get('id'))

        # Now get the bundles
        bundle = r.get(self.bundle.identity.id_,'/tmp/foo')

        self.assertIsNot(bundle, None)
        self.assertEquals('a1qSlv',bundle.identity.id_)

        # Should show up in datasets list. 
        o = r.datasets()
        
        self.assertTrue('a1qSlv' in o.keys() )
    
        o = r.find(r.query().table(name='tone').partition(any=True))
      
        self.assertTrue( 'b1qSlv001' in [i.Partition.id_ for i in o])
        self.assertTrue( 'a1qSlv' in [i.Dataset.id_ for i in o])
      

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()