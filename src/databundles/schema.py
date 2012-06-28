'''
Created on Jun 23, 2012

@author: eric
'''

class Schema(object):
    
     
    def __init__(self, bundle):
        self.bundle = bundle
        
        if not self.bundle.identity.id_:
            raise ValueError("self.bundle.identity.oid not set")
        
        self.dataset_id = self.bundle.identity.id_
        
      
    def generate(self):
        '''Load the schema from the bundle.schemaGenerator() generator'''
        from databundles.orm import Table, Column
      
        s = self.bundle.database.session
       
        
        s.query(Table).delete()
        s.query(Column).delete()
       
        for i in self.bundle.schemaGenerator():       
            if isinstance(i, Table):
                self.add_table(i)
            elif isinstance(i, Column):
                self.add_column(i.table_name, i)
        
        s.commit()
        
    @property
    def tables(self):
        '''Return a list of tables for this bundle'''
        from databundles.orm import Table
        return (self.bundle.database.session.query(Table)
                .filter(Table.d_id==self.bundle.identity.id_)
                .all())
    
    def table(self, name_or_id):
        '''Return an orm.Table object, from either the id or name'''
        from databundles.orm import Table
        import sqlalchemy.orm.exc
        
        try:
            return (self.bundle.database.session.query(Table)
                    .filter(Table.id_==name_or_id).one)
        except sqlalchemy.orm.exc.NoResultFound:
            return (self.bundle.database.session.query(Table)
                    .filter(Table.name==name_or_id).one)
    
    
    def add_table(self, name_or_table, **kwargs):
        '''Add a table to the schema'''
        from databundles.orm import Table
        import sqlalchemy.orm.exc

        # if name is a Table object, extract the dict and name
        if isinstance(name_or_table, Table):
            kwargs = name_or_table.__dict__
            name = kwargs.get("name",None) 
        else:
            name = name_or_table
            
        name = Table.mangle_name(name)
        
        s = self.bundle.database.session
        
        try:    
            row = (s.query(Table)
                   .filter(Table.name==name)
                   .filter(Table.d_id==self.dataset_id).one())
 
        except sqlalchemy.orm.exc.NoResultFound:

            row = Table(name=name, d_id=self.dataset_id)
            s.add(row)
            
        for key, value in kwargs.items():    
            if key[0] != '_' and key not in ['d_id','name','sequence_id']:
                #print 'Setting', self.dataset_id, key,value
                setattr(row, key, value)
      
        return row
        
    def add_column(self, table_name, name, **kwargs):
        '''Add a column to the schema'''
    
        if not table_name:
            raise ValueError("Must supply a table_name")
    
        # Will fetch the table, or create if not exists
        t = self.add_table(table_name)
       
        return t.add_column(name, **kwargs)
        
    
    @property
    def columns(self):
        '''Return a list of tables for this bundle'''
        from databundles.orm import Column
        return (self.bundle.database.session.query(Column).all())
        
    def get_table_meta(self, name):
        s = self.bundle.database.session
        from databundles.orm import Table, Column
        
        import sqlalchemy
        from sqlalchemy import MetaData   
        from sqlalchemy import Column as SAColumn
        from sqlalchemy import Table as SATable
        
        type_map = { 
        None: sqlalchemy.types.Text,
        Column.DATATYPE_TEXT: sqlalchemy.types.Text,
        Column.DATATYPE_INTEGER:sqlalchemy.types.Integer,
        Column.DATATYPE_REAL:sqlalchemy.types.Float,     
        Column.DATATYPE_DATE: sqlalchemy.types.Date,
        Column.DATATYPE_TIME:sqlalchemy.types.Time,
        Column.DATATYPE_TIMESTAMP:sqlalchemy.types.DateTime,
        }
     
        def translate_type(column):
            # Creates a lot of unnecessary objects, but spped is not important here. 
            
            type_map[Column.DATATYPE_NUMERIC] = sqlalchemy.types.Numeric(column.precision, column.scale),
            
            return type_map[column.datatype]
        
        metadata = MetaData()
        
        q =  (s.query(Table)
                   .filter(Table.name==name)
                   .filter(Table.d_id==self.dataset_id))
      
       
        table = q.one()
        
        at = SATable(table.name, metadata)
 
        for column in table.columns:
            ac = SAColumn(column.name, translate_type(column), primary_key = False)

            at.append_column(ac);
    
        return metadata, at
        
        
        