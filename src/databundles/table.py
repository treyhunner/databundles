'''
Created on Jun 13, 2012

@author: eric
'''

class Table(object):
    '''
    classdocs
    '''
     
    from databundles.properties import SimpleProperty
    
    name = SimpleProperty(None)
    altname = SimpleProperty(None)
    onid = SimpleProperty(None)
    description = SimpleProperty(None)
    keywords = SimpleProperty(None)
    
    columns_ = dict()

    def __init__(self, **kwargs):
        '''
        Constructor
        '''
    
        self.name =  kwargs.get("name",None)
        self.altname =  kwargs.get("altname",None)
        self.onid =  kwargs.get("onid",None)
        self.description =  kwargs.get("description",None)
        self.keywords =  kwargs.get("keywords",None)
    
    @property
    def columns(self):
        return self.columns_
    
    def add_column(self, column):
        from databundles.column import Column
        
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
        
        for name,column in self.columns_.items():
            
            x += "\n-------- "+str(column.name)+"\n"
            x += str(column)  
    
        return x