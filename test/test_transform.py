'''
Created on Aug 31, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.run import  RunConfig
from test_base import  TestBase

class Test(TestBase):

    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir


        
    def tearDown(self):
        pass

    def test_transforms(self):
        from databundles.transform import  CensusTransform  
           
        #        
        #all_id               INTEGER        
        #text1                TEXT       3    NONE
        #text2                TEXT       4    NONE
        #integer1             INTEGER    3    -1
        #integer2             INTEGER    4    -1
        #integer3             INTEGER    5    -1
        #float                REAL       5    -1
        
        processors = {}
        for table in self.bundle.schema.tables:
            source_cols = [c.name for c in table.columns]
            
            columns = [c for c in table.columns if c.name in source_cols  ]  
            prow = [CensusTransform(c, useIndex=True) for c in columns]
     
            processors[table.name] = prow
            
        
        
        rows = [
                #[None, 1,2,3,4,5,6],
                [101, 999,9999,9999,9999,9999,6.34],
                ['101', '999','9999','9999','9999','9999','6.34']
                ]
        
        for row in rows:
            values=[ f(row) for f in processors['all'] ]
            print values
        
           
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())