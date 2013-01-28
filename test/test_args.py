'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig

server_url = 'http://localhost:7979'

class Test(unittest.TestCase):


    def setUp(self):
        bundle = Bundle()      
        bundle.clean()
        
        self.bundle = Bundle()
        
   
        
    def tearDown(self):
        pass

    def test_transforms(self):

        print self.bundle.parse_args('clean parse build'.split(' '))
 
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())