'''
Created on Jun 9, 2012

@author: eric
'''
import unittest


class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testName(self):
        import pprint
        
        from databundles.objectnumber import ObjectNumber
 
        o = ObjectNumber()
 
        pprint.pprint(ObjectNumber.TYPE.DATASET)
        
        o.TYPE.DATASET = 'z'
       
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()