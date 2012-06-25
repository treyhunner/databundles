'''
Created on Jun 13, 2012

@author: eric
'''
import unittest
from databundles.objectnumber import ObjectNumber

class Test(unittest.TestCase):

    def test_basic(self):
      
        self.assertEquals('a1',str(ObjectNumber(1)))
        self.assertEquals('b101',str(ObjectNumber(1,1)))
        self.assertEquals('c10101',str(ObjectNumber(1,1,1)))

        with self.assertRaises(ValueError):
            self.assertEquals('aFooBar',str(ObjectNumber('FooBar')))
      
        
        self.assertEquals('aFooBar',str(ObjectNumber('aFooBar')))
        self.assertEquals('aFooBar',str(ObjectNumber(ObjectNumber('aFooBar'))))
 
        on = ObjectNumber('aFooBar')

        self.assertEquals('bFooBar00',str(ObjectNumber(on,0)))
        self.assertEquals('cFooBar0000',str(ObjectNumber(on,0,0)))
        self.assertEquals('bFooBarZZ',str(ObjectNumber(on,3843)))
        self.assertEquals('cFooBarZZZZ',str(ObjectNumber(on,3843,3843)))
        
        with self.assertRaises(ValueError):
            on = ObjectNumber(on,3844)
            print str(on)
     
        with self.assertRaises(ValueError):
            on = ObjectNumber(on,3844,3844)
            print str(on)
     
        o = ObjectNumber('aFooBar')
        self.assertIsNone(o.table);
        self.assertIsNone(o.column);
        
        o = ObjectNumber('bFooBar03')
        self.assertEquals(3,o.table);
        self.assertIsNone(o.column);
        
        o = ObjectNumber('cFooBar0302')
        self.assertEquals(3,o.table);
        self.assertEquals(2,o.column);
        
        o = ObjectNumber('cFooBar0302',20)
        o.type = ObjectNumber.TYPE.TABLE
        self.assertEquals(20,o.table);
        self.assertEquals('bFooBar0k',str(o))
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()