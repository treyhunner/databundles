'''
Created on Jun 13, 2012

@author: eric
'''

class Column(object):
    '''
    classdocs
    '''

        
    from databundles.properties import SimpleProperty
    
    name = SimpleProperty(None)
    altname = SimpleProperty(None)
    datatype = SimpleProperty(None)
    size = SimpleProperty(None)
    precision = SimpleProperty(None)
    scale = SimpleProperty(None)
    constraints = SimpleProperty(None)
    flags = SimpleProperty(None)
    onid = SimpleProperty(None)
    description = SimpleProperty(None)
    keywords = SimpleProperty(None)
    measure = SimpleProperty(None)
    units = SimpleProperty(None)
    universe = SimpleProperty(None)
    uscale = SimpleProperty(None)

    table = None

    def __init__(self, table=None, **kwargs):
        '''
        Constructor
        '''
          
        from table import Table
        
        if table and not isinstance(table, Table ):
            raise TypeError, "table must be of type Table. Got: "+str(type(table))
        
        self.table = table
        
        self.name =  kwargs.get("name",None)
        self.altname =  kwargs.get("altname",None)
        self.onid =  kwargs.get("onid",None)
        self.description =  kwargs.get("description",None)
        self.keywords =  kwargs.get("keywords",None)
        
        self.datatype =  kwargs.get("datatype",None)
        self.size =  kwargs.get("size",None)
        self.precision =  kwargs.get("precision",None)
        self.scale =  kwargs.get("scale",None)
        self.constraints =  kwargs.get("constraints",None)
        self.flags =  kwargs.get("flags",None)
        self.measure =  kwargs.get("measure",None)
        self.units =  kwargs.get("units",None)
        self.universe =  kwargs.get("universe",None)
        self.uscale =  kwargs.get("uscale",None)

    def __str__(self):
        return ("name: {0.name}\n"+
            "altname: {0.altname}\n"+
            "datatype: {0.datatype}\n"+
            "size: {0.size}\n"+
            "precision: {0.precision}\n"+
            "scale: {0.scale}\n"+
            "constraints: {0.constraints}\n"+
            "flags: {0.flags}\n"+
            "onid: {0.onid}\n"+
            "description: {0.description}\n"+
            "keywords: {0.keywords}\n"+
            "measure: {0.measure}\n"+
            "units: {0.units}\n"+
            "universe: {0.universe}\n"+
            "uscale: {0.uscale}\n").format(self)
        
        
    