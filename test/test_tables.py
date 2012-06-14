'''
Created on Jun 14, 2012

@author: eric
'''
import unittest


class Test(unittest.TestCase):


    def test_basic(self):
        from databundles.table import Table
        from databundles.column import Column
        import uuid
        
        t = Table()
        t.name = "Test Table"
        t.altname = 'Alt Name'
      
    
        t.add_column(Column(name=str(uuid.uuid4()),altname='foobar'))
        t.add_column(Column(name=str(uuid.uuid4()),altname='foobar'))
        t.add_column(Column(name=str(uuid.uuid4()),altname='foobar'))
        
        print t.dump()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_basic']
    unittest.main()