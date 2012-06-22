'''
Created on Jun 22, 2012

@author: eric
'''
import unittest
from  databundles.bundle import Bundle

class Test(unittest.TestCase):

    def setUp(self):
        import os
        bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_files')
        bundle = Bundle(bundle_dir)     
        bundle.database.delete()
     
        self.bundle = Bundle(bundle_dir)
      
    def test_basic(self):
        print self.bundle.identity.source
        print self.bundle.identity.dataset
        print self.bundle.identity.creator
        print self.bundle.identity.creatorcode
        print self.bundle.identity.name

        self.bundle.identity.source = 'foobar'
        
        print self.bundle.identity.name


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()