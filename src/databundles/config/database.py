'''
Created on Jun 13, 2012

@author: eric
'''


from databundles.properties import SimpleProperty
  
class Column(object):
    '''
    classdocs
    '''
    
    name = SimpleProperty("name",None)
    altname = SimpleProperty("altname",None)
    datatype = SimpleProperty("datatype",None)
    size = SimpleProperty("size",None)
    precision = SimpleProperty("precision",None)
    scale = SimpleProperty("scale",None)
    constraints = SimpleProperty("constraints",None)
    flags = SimpleProperty("flags",None)
    onid = SimpleProperty("onid",None)
    description = SimpleProperty("description",None)
    keywords = SimpleProperty("keywords",None)
    measure = SimpleProperty("measure",None)
    units = SimpleProperty("units",None)
    universe = SimpleProperty("universe",None)
    uscale = SimpleProperty("uscale",None)

    table = None

    def __init__(self, table=None, **kwargs):
        '''
        Constructor
        '''
         
        if table and not isinstance(table, Table ):
            raise TypeError, "table must be of type Table. Got: "+str(type(table))
        
        self.table = table
           
        # Set all of the SimpleProperties from kwargs
        for name,value in Column.__dict__.items():
            if isinstance(value, SimpleProperty) and kwargs.get(name,None) != None:
                setattr(self, name, kwargs.get(name,None))


    def __str__(self):
        
        out = '';
     
        for name,value in Column.__dict__.items():
            if isinstance(value, SimpleProperty) and getattr(self,name) != None:
                out += name+": "+getattr(self,name)+"\n";
            
        return out
    
    

class Table(object):
    '''
    classdocs
    '''

    name = SimpleProperty('name',None)
    altname = SimpleProperty('altname',None)
    onid = SimpleProperty('onid',None)
    description = SimpleProperty('description',None)
    keywords = SimpleProperty('keywords',None)
    
  
    columns_ = dict()

    def __init__(self, database=None, **kwargs):
        '''
        Constructor
        '''
        if database and not isinstance(database, Database ):
            raise TypeError, "table must be of type Database. Got: "+str(type(database))
        
        self.database = database
    
        # Set all of the SimpleProperties from kwargs
        for name,value in Column.__dict__.items():
            if isinstance(value, SimpleProperty) and kwargs.get(name,None) != None:
                setattr(self, name, kwargs.get(name,None))
        
        
    @property
    def columns(self):
        return self.columns_
    
    def add_column(self, column):
        """Add a column to the table"""
      
        
        if not isinstance(column, Column):
            raise TypeError, "column must be of type Column. Got: "+str(type(column))
        
        column.table = self
        
        self.columns_[column.name] = column
    
       
    def __str__(self):
        return (
            "onid: {0.onid}\n"+
            "name: {0.name}\n"+
            "altname: {0.altname}\n"+
            "description: {0.description}\n"+
            "keywords: {0.keywords}\n").format(self) 
       
    def dump(self):
        x = str(self) 
        
        for column in self.columns_.values():
            x += "\n-------- "+str(column.name)+"\n"
            x += str(column)  
    
        return x
    
class Database(object):
    
    tables = []
    
    def __init__(self, **kwargs):
        '''Represents the configuration for the database structure for a dataset.
        It is not an actual database, only the package schem for it, which includes
        meta data that is addtional to the normal SQL tables definitions. '''
        
    def add_table(self, table):
        """Add a table to the database configuration"""
      
        
        if not isinstance(table, Table):
            raise TypeError, "column must be of type Table. Got: "+str(type(table))
        
        table.database = self
        
        self.columns_[table.name] = table 
        
        
    def set_id(self,onid=None):
        """Set a dataset id for all tables and columns. If id is None, create a new dataset Id """
        
        for table in self.table:
            table.set_id(onid)
            
        
