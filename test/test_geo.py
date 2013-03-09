'''
Created on Jan 17, 2013

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.identity import * #@UnusedWildImport
from test_base import  TestBase
from osgeo.gdalconst import GDT_Float32

import ogr

class Test(TestBase):
 
    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir


    def tearDown(self):
        pass

    def test_basic(self):
        from databundles.geo.analysisarea import get_analysis_area, create_bb,  draw_edges
        from databundles.geo import Point
        from databundles.geo.kernel import GaussianKernel
             
        aa = get_analysis_area(self.bundle.library, geoid = '0666000')
        
        a = aa.new_array()

        #draw_edges(a)
        print a.shape, a.size
        
        print a
        
        gaussian = GaussianKernel(11,6)
        
        for i in range(0,400, 20):
            p = Point(100+i,100+i)
            gaussian.apply_add(a,p)
         
        
        aa.write_geotiff('/tmp/box.tiff',  a,  type_=GDT_Float32)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()