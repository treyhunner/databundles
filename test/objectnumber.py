'''
Created on Jun 13, 2012

@author: eric
'''
import unittest


class Test(unittest.TestCase):


    def test_basic(self):
        from databundles.objectnumber import ObjectNumber
        
        on1 = ObjectNumber(1)
        
        print "On 1: "+str(on1)
        
        on2 = ObjectNumber()
        
        print "On Empty: "+str(on2)
        
        on = ObjectNumber(on2)
        
        print "On Copy: "+str(on)
        
        on = ObjectNumber(on2,10)
        
        print "On Copy: "+str(on)

        on = ObjectNumber(on,10,20)
        
        print "On Copy: "+str(on)
        
        on = ObjectNumber(on,10,3843)
        
        print "On Copy: "+str(on)
        
    def test_values(self):
        from databundles.objectnumber import ObjectNumber
        
        print ObjectNumber('foo')
        
        print ObjectNumber('foo',1)
        
        print ObjectNumber('foo',2)
        
        print ObjectNumber('foo',1,3)
        
        print ObjectNumber('foo',2,4)
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()