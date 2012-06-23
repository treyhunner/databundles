'''
Created on Jun 23, 2012

@author: eric
'''

class Schema(object):
    
     
    def __init__(self, bundle):
        self.bundle = bundle
        
      
    def generate(self):
        '''Load the schema from the bundle.schemaGenerator() generator'''
        from databundles.orm import Table, Column
      
        for i in self.bundle.schemaGenerator():       
            if isinstance(i, Table):
                self.add_table(i)
            elif isinstance(i, Column):
                self.add_column(i.table_name, i)
        
        
    @property
    def tables(self):
        '''Return a list of tables for this bundle'''
        from databundles.orm import Table
        return (self.bundle.database.session.query(Table)
                .filter(Table.d_id==self.bundle.identity.oid)
                .all())
    
    def add_table(self, name_or_table, **kwargs):
        '''Add a table to the schema'''
        from databundles.orm import Table
        
        if not self.bundle.identity.oid:
            raise ValueError("self.bundle.identity.oid not set")
        
        # if name is a Table object, extract the dict and name
        if isinstance(name_or_table, Table):
            kwargs = name_or_table.__dict__
            name = kwargs.get("name",None) 
        else:
            name = name_or_table
            
        s = self.bundle.database.session
        
        try:
            row = (s.query(Table)
                   .filter(Table.name==name)
                   .filter(Table.d_id==self.bundle.identity.oid)
                   .one())
        except:
            row = Table(name=name, d_id=self.bundle.identity.oid)
            s.add(row)
            
        for key, value in kwargs.items():    
            if key[0] != '_' and key not in ['d_id','name','sequence_id']:
                #print 'Setting', self.bundle.identity.oid, key,value
                setattr(row, key, value)
      
        s.commit()
        
        return row
        
    def add_column(self, table_name, name, **kwargs):
        '''Add a column to the schema'''
    
        # Will fetch the table, or create if not exists
        t = self.add_table(table_name)
       
        return t.add_column(name, **kwargs)
        
    
    @property
    def columns(self):
        pass