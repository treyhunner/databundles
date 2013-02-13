'''
Created on Feb 12, 2013

@author: eric
'''
        
from databundles.identity import PartitionIdentity
from osgeo import gdal, gdal_array, osr
from osgeo.gdalconst import GDT_Float32, GDT_Byte, GDT_Int16
from numpy  import *
  
class GaussianMatrix(object):
    
    def __init__(self, size=9, fwhm=3 ): 
        m = self.makeGaussian(size, fwhm)

        m /= sum(m) # Normalize the sum of all cells in the matrix to 1

        self.offset = (m.shape[0] - 1) / 2 
        self.matrix = m

    @staticmethod
    def makeGaussian(size, fwhm = 3):
        """ Make a square gaussian kernel.
    
        size is the length of a side of the square
        fwhm is full-width-half-maximum, which
        can be thought of as an effective radius.
        """
    
        x = arange(0, size, 1, float32)
        y = x[:,newaxis]
        x0 = y0 = size // 2
        return exp(-4*log(2) * ((x-x0)**2 + (y-y0)**2) / fwhm**2)
           
class LinearMatrix(object):
    
    def __init__(self): 
        ma = array([.5,1.,2.,5.,9.,5.,2.,1.,.5]) 
        m =  outer(ma,ma) 
          
        
        m /= sum(m) # Normalize the sum of all cells in the matrix to 1

        # This will usually cut off the corners, creating a more
        # round shape. 
        for (x_m,y_m), value in ndenumerate(m):
            if m[y_m][x_m] < .001:
                m[y_m][x_m] = 0

        self.offset = (m.shape[0] - 1) / 2 
        self.matrix = m
                    
class DensityImage(object):
    '''
    classdocs
    '''

    def __init__(self, partition, bin_scale, matrix = None):
        '''
        Constructor
        '''
        
        self.bin_scale =float(bin_scale)
        self.i_bin_scale = 1. / float(bin_scale)
        
        
        self.m = matrix
        
        if self.m:
            mo = self.m.offset
        else:
            mo = 0
        
        self.output_name = partition.table.name+".tiff"

        self.bb = partition.extents

        
        self.x_offset_c = int(self.bb.min_x*self.bin_scale)-mo
        self.y_offset_c = int(self.bb.min_y*self.bin_scale)-mo

        self.x_max_c = int(self.bb.max_x*self.bin_scale)
        self.y_max_c = int(self.bb.max_y*self.bin_scale)    

        self.x_offset_d = self.x_offset_c/self.bin_scale
        self.y_offset_d = self.y_offset_c/self.bin_scale
       
        # Size of the output array. The '1' handles rounding down, 
        self.x_size = self.x_max_c - self.x_offset_c + 1 + mo
        self.y_size = self.y_max_c - self.y_offset_c + 1 + mo
   
        self.a =  zeros( (  self.y_size, self.x_size),  dtype=float )
      
    def info(self):
        print 'OFFSETS', self.x_offset_d, self.y_offset_d
        print "size",  self.x_size, self.y_size
        print 'UL Corner (x,y)',self.bb.min_x, self.bb.max_y
        print 'LR Corner (x,y)',self.bb.max_x, self.bb.min_y
       
    def add_count(self, lon,lat,v=1):
        x = int(lon*self.bin_scale) - self.x_offset_c 
        y = int(lat*self.bin_scale) - self.y_offset_c 
        
        x -= 1
        
        self.a[y,x] += v

                
    def add_matrix(self, lon,lat):  
        
        if self.m is None:
            return self.add_count(lon,lat,v=1)
         
        x = int(lon*self.bin_scale) - self.x_offset_c 
        y = int(lat*self.bin_scale) - self.y_offset_c 
        
        x -= 1
        
        # Add in smoothing matrix
        for (x_m,y_m), value in ndenumerate(self.m.matrix):
            #print x,x_m,x+x_m,' : ',y,y_m,y+y_m
            self.a[y+y_m-self.m.offset][x+x_m-self.m.offset] += value
  

    def draw_registration(self):
        # Draw registration marks at the corners. 
        a = self.a
        y_size = a.y_size
        x_size = a.x_size

        for i in range(0,30,2):
            a[i,0] = 1
            a[0,i] = 1
            
            a[y_size-i-1,x_size-1] = 1
            a[y_size-1,x_size-i-1] = 1
            
            a[y_size-i-1,0] = 1
            a[0,x_size-i-1] = 1
            
            a[i,x_size-1] = 1
            a[y_size-1,i] = 1
        
    def write(self,file_):
        
        driver = gdal.GetDriverByName('GTiff') 
        
            
        out = driver.Create(file_, self.a.shape[1], self.a.shape[0], 1, GDT_Float32)  
        
        transform = [ self.x_offset_d ,  # Upper Left X postion
                     self.i_bin_scale ,  # Pixel Width 
                     0 ,     # rotation, 0 if image is "north up" 
                     self.y_offset_d ,  # Upper Left Y Position
                     0 ,     # rotation, 0 if image is "north up"
                     self.i_bin_scale # Pixel Height
                     ]

        out.SetGeoTransform(transform)  
        
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326) # Lat/Long in WGS84
        out.SetProjection( srs.ExportToWkt() )
     
        out.GetRasterBand(1).SetNoDataValue(0)
        out.GetRasterBand(1).WriteArray(self.a)
      
        return file_