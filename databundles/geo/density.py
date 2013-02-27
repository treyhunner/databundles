'''
Created on Feb 12, 2013

@author: eric
'''
        
from databundles.identity import PartitionIdentity
from osgeo import gdal, gdal_array, osr
from osgeo.gdalconst import GDT_Float32, GDT_Byte, GDT_Int16
from numpy  import *
  
class Matrix(object):
  
    def round(self):
        '''Make the matrix spot sort of round, by masking values in the corners'''
        
        # Get the max value for the edge of the enclosed circle. 
        row_max = self.matrix[self.center][0]

        for (x_m,y_m), value in ndenumerate(self.matrix):
            if self.matrix[y_m][x_m] > row_max:
                self.matrix[y_m][x_m] = 0
        
    def norm(self):
        #self.matrix /= sum(self.matrix)
        self.matrix /= self.matrix.max()
        
        
    def invert(self):
        ''''Invert the values, so the cells closer to the center 
        have the higher values. '''
        #range = self.matrix.max() - self.matrix.min() 

        self.matrix = self.matrix.max() - self.matrix
        self.inverted = ~self.inverted
       
    def quantize(self, bins=255):
        from scipy.cluster.vq import kmeans, vq
        from util import jenks_breaks
        hist, edges = histogram(self.matrix.compressed(),bins=bins)
      
        print "Hist", hist
        print "Edges",edges
      
        centroids, distortions = kmeans(self.matrix, bins)
        code, dist = vq(self.matrix, centroids)
        print "Cent", code
        print "Dist", dist
        
        breaks = jenks_breaks(self.matrix.compressed().tolist(), bins)
        print "Breaks",breaks
        
        l = list(set(self.matrix.compressed().tolist()))
        l.sort()
        print "Uniques", l
        
        print self.matrix.compressed()
        digits = digitize(self.matrix.ravel(), breaks)
        
       
        print self.matrix.size
        print digits.size
        print self.matrix.shape[0]
        
        s = ma.array(reshape(digits, self.matrix.shape), mask=self.matrix.mask)
        
        print s
      
    def apply(self,a, x, y):
        # Add in smoothing matrix
        if self.inverted:
            f = min
        else:
            f = max
        
        for (y_m,x_m), value in ndenumerate(self.matrix):
            av =  a[y+y_m-self.center,x+x_m-self.center]
            a[y+y_m-self.center,x+x_m-self.center] = f(av, value)
   
     
class GaussianMatrix(Matrix):
    
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
        m =  ma.masked_array(exp(-4*log(2) * ((x-x0)**2 + (y-y0)**2) / fwhm**2))
        
        for (y,x), value in ndenumerate(m):
            if value < 1./1000:
                m[y, x] = 0
        
        return m
           
class LinearMatrix(Matrix):
    ''' Each cell is the distance, in cell widths, from the center '''
    def __init__(self, size): 
        import math
        if size%2 == 0:
            raise ValueError("Aray size must be odd")
        
        self.inverted = False
        
        self.matrix = ma.masked_array(zeros((size,size)), mask=True)
        
        # For 1-based array indexing, we'd hve to +1, but this is zero-based
        self.center = center = int(size/2) 
        
        row_max = size - center - 1 # Max value on a horix or vert edge
     
        for (x_m,y_m), value in ndenumerate(self.matrix):
                r  = sqrt( (y_m-center)**2 + (x_m-center)**2)
                if r <= row_max:
                    self.matrix[y_m,x_m] = r

                 
class DensityImage(object):
    '''
    classdocs
    '''

    def __init__(self,  extents, bin_scale, matrix=None ):
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
      
        self.bb = extents
        
        # Offsets to incorporate whole matrix, by expanding the
        # array a half matrix width in every direction
        # Turned it off to try just clipping. 
        #self.x_offset_c = int(self.bb.min_x*self.bin_scale)-mo
        #self.y_offset_c = int(self.bb.min_y*self.bin_scale)-mo
        
        # Offsets for corner of array in number of cells. 
        self.x_offset_c = int(self.bb.min_x*self.bin_scale)
        self.y_offset_c = int(self.bb.min_y*self.bin_scale)
        
        # Size of the array, in cells. 
        self.x_max_c = int(self.bb.max_x*self.bin_scale)
        self.y_max_c = int(self.bb.max_y*self.bin_scale)    

        # Offsets for the array, in units of the input
        self.x_offset_d = self.x_offset_c/self.bin_scale
        self.y_offset_d = self.y_offset_c/self.bin_scale
       
        # Size of the output array. The '1' handles rounding down, 
        #self.x_size = self.x_max_c - self.x_offset_c + 1 + mo
        #self.y_size = self.y_max_c - self.y_offset_c + 1 + mo
  
        # Size of the array, in units of cells. 
        self.x_size = self.x_max_c - self.x_offset_c 
        self.y_size = self.y_max_c - self.y_offset_c
   
        self.a =  zeros( (  self.y_size, self.x_size),  dtype=float)
  
        self.edge_errors = 0; # Number of values attempted to be written off edge of image
  
    def info(self):
        from numpy import histogram
        
        print 'OFFSETS       :',self.x_offset_d, self.y_offset_d
        print "size          :",self.x_size, self.y_size
        print 'UL Corner     :',self.bb.min_x, self.bb.max_y
        print 'LR Corner     :',self.bb.max_x, self.bb.min_y
        print "Value min, max:",self.a.min(),self.a.max()
        print "Elem Average  :",average(self.a)
        print "Masked Average:",ma.average(self.a)
        print "Mean          :",self.a.mean()
        print "Median        :",median(self.a)
        print "Std Dev       :",std(self.a)
        try:
            print "Histogram     :", histogram(self.a.compressed())[0].ravel().tolist()
        except:
            print "Histogram     :", histogram(self.a)[0].ravel().tolist()
       
    def add_count(self, x_in,y_in,v=1):
        x = int(x_in*self.bin_scale) - self.x_offset_c 
        y = int(y_in*self.bin_scale) - self.y_offset_c 
        
        x -= 1
      
        self.a[y,x] += v
            
    def add_matrix(self, x_in,y_in):  
        
        if self.m is None:
            return self.add_count(x_in,y_in,v=1)
         
        x = int(x_in*self.bin_scale) - self.x_offset_c 
        y = int(y_in*self.bin_scale) - self.y_offset_c 
        
        x -= 1
        
        # Add in smoothing matrix
        mat = self.m.matrix
        for (y_m,x_m), value in ndenumerate(mat):
            nx = x+x_m-self.m.offset
            ny = y+y_m-self.m.offset
            
            if nx < 0 or ny < 0:
                self.edge_errors += 1
                continue
            
            try:   
                self.a[ny,nx] += value
            except IndexError:
                # When writing off the edge of the array
                self.edge_errors += 1
      
    def mask(self):
        masked = ma.masked_equal(self.a,0)  
        self.a = masked
            
    def std_norm(self):
        """Normalize to +-4 sigma on the range 0 to 1"""

        mean = self.a.mean()
        std = self.a.std()
        self.a = (( self.a - mean) / std).clip(-4,4) # Def of z-score
        self.a += 4
        self.a /= 8
        
        try:
            self.a.set_fill_value(0)
        except AttributeError:
            # If it isn't a masked array
            pass


    def unity_norm(self):
        """scale to the range 0 to 1"""

        range  = self.a.max() - self.a.min()
        self.a = (self.a - self.a.min()) / range
   
        try:
            self.a.set_fill_value(0)
        except AttributeError:
            # If it isn't a masked array
            pass

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
        
    def quantize(self, bins):
        from util import jenks_breaks
        from numpy.random import choice #@UnresolvedImport
        
        print "Uniquing"
        unique_ = choice(unique(self.a), 1000)
        print "Uniques: ", unique_.size
        print "Breaking"
        breaks = jenks_breaks(unique_, bins)
        
        print "Breaks",breaks
        
        digitized = digitize(self.a.ravel(), breaks)
        
        self.a = ma.array(reshape(digitized, self.a.shape), mask=self.a.mask)

    def write(self,file_):
        
        driver = gdal.GetDriverByName('GTiff') 
        
            
        out = driver.Create(file_, self.a.shape[1], self.a.shape[0], 1, 
                            GDT_Float32, options = [ 'COMPRESS=LZW' ])  
        
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

    
