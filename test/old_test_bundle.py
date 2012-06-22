'''
Created on Jun 19, 2012

@author: eric
'''
import unittest
from  databundles.bundle import Bundle


class Test(unittest.TestCase):


    def setUp(self):
        import os
        bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_files')
        self.bundle = Bundle(bundle_dir)
       


    def tearDown(self):
        pass


    def testName(self):
        import pprint
        
        pprint.pprint(self.bundle.identity)
        
        print self.bundle.name
        
        self.bundle.partition.time = 'today'
        self.bundle.partition.space = None
        
        print self.bundle.name
        
        self.bundle.partition.time = None
        self.bundle.partition.space = None
        self.bundle.partition.table = None
        
        print self.bundle.name
        
        import copy
        
        b = copy.deepcopy(self.bundle)
        b.partition.space = 'foobar'
        
        print b.name
        
    def test_path(self):
        
        print self.bundle.path('foo')
        print self.bundle.path('foo','bar','baz')
        
        
    def test_orm(self):
        print self.bundle.name
        
        from databundles.config.orm import Dataset, Table, Column
        from databundles.objectnumber import  ObjectNumber
        
        self.bundle.protodb.create()
        self.bundle.database.create()

        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=self.bundle.database.engine)
        session = Session()

        session.query(Dataset).delete()
        session.query(Table).delete()

        ds = Dataset(oid = str(ObjectNumber()), name='a', source='foobar')
 
        session.add(ds)
        
        ds.tables.append(Table(oid='1',name='nar'))
        
        session.commit()
        

        ds = session.query(Dataset).first()
        
        ##session.delete(ds)
        
        ds.oid = 'foobar'
        
        
        session.commit()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()