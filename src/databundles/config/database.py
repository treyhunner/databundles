'''
Created on Jun 13, 2012

@author: eric
'''

from databundles.properties import SimpleProperty
from databundles.config.error import ConfigError
from databundles.objectnumber import ObjectNumber

class _const:
        class ConstError(TypeError): pass
        def __setattr__(self,name,value):
            if self.__dict__.has_key(name):
                raise self.ConstError, "Can't rebind const(%s)"%name
            self.__dict__[name]=value

class Column(object):
    '''
    classdocs
    '''
    
   
    DATATYPE_TEXT = 'text'
    DATATYPE_INTEGER ='integer' 
    DATATYPE_REAL = 'real'
    DATATYPE_NUMERIC = 'numeric'
    DATATYPE_DATE = 'date'
    DATATYPE_TIME = 'time'
    DATATYPE_TIMESTAMP = 'timestamp'
    
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

    

    def __init__(self, table=None, **kwargs):
        '''
        Constructor
        '''
        self.table = None
         
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
    
    def as_dict(self):
        d = {}
        for name,value in Column.__dict__.items():
            if isinstance(value, SimpleProperty) and getattr(self,name) != None:
                d[name] = getattr(self,name)
               
        # 'onid' is internal, because .id is reserved.  
        d['id'] = str(self.onid)
        del d['onid']
        return d
                

      
class Table(object):
    '''
    classdocs
    '''

    name = SimpleProperty('name',None)
    altname = SimpleProperty('altname',None)
    onid = SimpleProperty('onid',None)
    description = SimpleProperty('description',None)
    keywords = SimpleProperty('keywords',None)
    universe = SimpleProperty('universe',None)
    
   

    def __init__(self,  **kwargs):
        '''
        Constructor
        '''
       
        self. columns_ = []
       
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
        
        self.columns_.append(column)
    
    def propagate_id(self):
        """Propagate the object id of this table to each of the columns"""
        import copy
        
        for index,column in enumerate(self.columns_):
            onid = copy.copy(self.onid)
            onid.column = index+1
            column.onid = onid
       
    def __str__(self):
        return (
            "onid: {0.onid}\n"+
            "name: {0.name}\n"+
            "altname: {0.altname}\n"+
            "description: {0.description}\n"+
            "keywords: {0.keywords}\n").format(self) 
       
    def dump(self):
        x = "Table: {} {} ({})\n".format(self.name, self.altname, self.onid)
        
        for column in self.columns_:
            x += "  Col: {} {} ({})".format(column.name, column.datatype, column.onid)
            x += "\n"
    
        return x
    
    def as_dict(self):
        d = {}
        for name,value in Table.__dict__.items():
            if isinstance(value, SimpleProperty) and getattr(self,name) != None:
                d[name] =  getattr(self,name)
        # 'id' is reserved in Python 
        d['id'] = str(self.onid) 
        del d['onid']
          
        d['columns']  = []   
        for column in self.columns_:
            d['columns'].append(column.as_dict())
            
      
        return d
  
    
class Database(object):
    
    tables_ = []
    onid = ObjectNumber()
    
    def __init__(self, **kwargs):
        """Represents the configuration for the database structure for a dataset.
        It is not an actual database, only the package schem for it, which includes
        meta data that is addtional to the normal SQL tables definitions."""
        
    def add_table(self, table):
        """Add a table to the database configuration"""
      
        if not isinstance(table, Table):
            raise TypeError, "column must be of type Table. Got: "+str(type(table))
        
        table.database = self
        
        if not table.name:
            raise ConfigError, "Table did not define name"
            
        
        self.tables_.append(table) 
        
        return table
        
        
    @property
    def tables(self):
        """A list of tables in this database"""
        return self.tables_
        
    def propagate_id(self):
        """Set a dataset id for all tables and columns. If id is None, create a new dataset Id """
        import copy
        
        for index, table  in enumerate(self.tables_):
            
            onid = copy.copy(self.onid)
            onid.table = index+1
            onid.column = None
            table.onid = onid
            
            table.propagate_id()
            
         
    def dump(self):
        """Return a string for debugging"""
        x = '';
        for table in self.tables_:
            x += table.dump()
            x += "\n"
        
        return x

    def as_dict(self):
        """Return the database, and all of its tables, as a python structure that is
        suitable for conversion to YAML"""
        
        a = []
        for table in self.tables_:
            a.append(table.as_dict())
        
        return a
    
    def from_dict(self, d):
        """Set all tables and columns from a python data structure, as returned from loading YAML"""
        pass
    
    
   
            
           
