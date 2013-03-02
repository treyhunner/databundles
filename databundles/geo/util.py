'''
Created on Feb 15, 2013

@author: eric
'''
from collections import namedtuple
import random
from osgeo import gdal, ogr    

BoundingBox = namedtuple('BoundingBox', ['min_x', 'min_y','max_x', 'max_y'])

def extents(database, table_name, where=None, lat_col='_db_lat', lon_col='_db_lon'):

    '''Return the bounding box for a table in the database. The partition must specify 
    a table
    
    '''
    # Find the extents of the data and figure out the offsets for the array. 
    e= database.connection.execute
    
    if where:
        where = "WHERE "+where
    else:
        where = ''
    
    r = e("""SELECT min({lon}) as min_x, min({lat}) as min_y, 
            max({lon}) as max_x, max({lat}) as max_y from {table} {where}"""
            .format(lat=lat_col, lon=lon_col, table=table_name, where=where)
        ).first()
          
    # Convert to a regular tuple 
    o = BoundingBox(r[0], r[1],r[2],r[3])
    
    return o

#From http://danieljlewis.org/files/2010/06/Jenks.pdf
#
# !!!! Use psal instead!
# !!!! http://pysal.geodacenter.org/1.2/library/esda/mapclassify.html#pysal.esda.mapclassify.Natural_Breaks
#
def jenks_breaks(dataList, numClass): 
 
    dataList.sort() 
    
    print "A"
    mat1 = [] 
    for i in range(0, len(dataList) + 1): 
        temp = [] 
        for j in range(0, numClass + 1): 
            temp.append(0) 
        mat1.append(temp) 

    print "B"
    mat2 = [] 
    for i in range(0, len(dataList) + 1): 
        temp = [] 
        for j in range(0, numClass + 1): 
            temp.append(0) 
        mat2.append(temp) 
  
    print "C"
    for i in range(1, numClass + 1): 
        mat1[1][i] = 1 
        mat2[1][i] = 0 
        for j in range(2, len(dataList) + 1): 
            mat2[j][i] = float('inf') 

    print "D"
    v = 0.0 
    # # iterations = datalist * .5*datalist * Numclass
    for l in range(2, len(dataList) + 1): 
        s1 = 0.0 
        s2 = 0.0 
        w = 0.0 
        for m in range(1, l + 1): 
            i3 = l - m + 1 
    
            val = float(dataList[i3 - 1]) 
    
            s2 += val * val 
            s1 += val 
    
            w += 1 
            v = s2 - (s1 * s1) / w 
            i4 = i3 - 1 
    
            if i4 != 0: 
                for j in range(2, numClass + 1): 
                    if mat2[l][j] >= (v + mat2[i4][j - 1]): 
                        mat1[l][j] = i3 
                        mat2[l][j] = v + mat2[i4][j - 1] 
        mat1[l][1] = 1 
        mat2[l][1] = v 
    k = len(dataList) 
    kclass = [] 
    print "E"
    for i in range(0, numClass + 1): 
        kclass.append(0) 
    
    kclass[numClass] = float(dataList[len(dataList) - 1]) 
    
    countNum = numClass 
    
    print 'F'
    while countNum >= 2: 
        #print "rank = " + str(mat1[k][countNum]) 
        id_ = int((mat1[k][countNum]) - 2) 
        #print "val = " + str(dataList[id]) 
    
        kclass[countNum - 1] = dataList[id_] 
        k = int((mat1[k][countNum] - 1)) 
        countNum -= 1 
    
    return kclass 
 
def getGVF( dataList, numClass ): 
    """ The Goodness of Variance Fit (GVF) is found by taking the 
    difference between the squared deviations from the array mean (SDAM) 
    and the squared deviations from the class means (SDCM), and dividing by the SDAM 
    """ 
    breaks = jenks_breaks(dataList, numClass) 
    dataList.sort() 
    listMean = sum(dataList)/len(dataList) 
    print listMean 
    SDAM = 0.0 
    for i in range(0,len(dataList)): 
            sqDev = (dataList[i] - listMean)**2 
            SDAM += sqDev 
             
    SDCM = 0.0 
    for i in range(0,numClass): 
            if breaks[i] == 0: 
                    classStart = 0 
            else: 
                    classStart = dataList.index(breaks[i]) 
                    classStart += 1 
            classEnd = dataList.index(breaks[i+1]) 
    
            classList = dataList[classStart:classEnd+1] 
    
            classMean = sum(classList)/len(classList) 
            print classMean 
            preSDCM = 0.0 
            for j in range(0,len(classList)): 
                    sqDev2 = (classList[j] - classMean)**2 
                    preSDCM += sqDev2 
    
            SDCM += preSDCM 
    
    return (SDAM - SDCM)/SDAM 


def rasterize(pixel_size=25):
    # Open the data source
    
    RASTERIZE_COLOR_FIELD = "__color__"
    
    orig_data_source = ogr.Open("test.shp")
    # Make a copy of the layer's data source because we'll need to 
    # modify its attributes table
    source_ds = ogr.GetDriverByName("Memory").CopyDataSource(orig_data_source, "")
    source_layer = source_ds.GetLayer(0)
    source_srs = source_layer.GetSpatialRef()
    x_min, x_max, y_min, y_max = source_layer.GetExtent()
    
    # Create a field in the source layer to hold the features colors
    field_def = ogr.FieldDefn(RASTERIZE_COLOR_FIELD, ogr.OFTReal)
    source_layer.CreateField(field_def)
    source_layer_def = source_layer.GetLayerDefn()
    field_index = source_layer_def.GetFieldIndex(RASTERIZE_COLOR_FIELD)
    
    # Generate random values for the color field (it's here that the value
    # of the attribute should be used, but you get the idea)
    
    for feature in source_layer:
        feature.SetField(field_index, random.randint(0, 255))
        source_layer.SetFeature(feature)
        
    # Create the destination data source
    x_res = int((x_max - x_min) / pixel_size)
    y_res = int((y_max - y_min) / pixel_size)
    target_ds = gdal.GetDriverByName('GTiff').Create('test.tif', x_res,
            y_res, 3, gdal.GDT_Byte)
    
    target_ds.SetGeoTransform(( x_min, pixel_size, 0, y_max, 0, -pixel_size,))
    
    if source_srs:
        # Make the target raster have the same projection as the source
        target_ds.SetProjection(source_srs.ExportToWkt())
    else:
        # Source has no projection (needs GDAL >= 1.7.0 to work)
        target_ds.SetProjection('LOCAL_CS["arbitrary"]')
        
    # Rasterize
    err = gdal.RasterizeLayer(target_ds, (3, 2, 1), source_layer,
            burn_values=(0, 0, 0),
            options=["ATTRIBUTE=%s" % RASTERIZE_COLOR_FIELD])
    
    if err != 0:
        raise Exception("error rasterizing layer: %s" % err)


