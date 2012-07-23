'''
Created on Jun 23, 2012

@author: eric
'''

class Schema(object):
    
     
    def __init__(self, bundle):
        from partition import  Partition
        self.bundle = bundle # COuld also be a partition
        
        # the value for a Partition will be a PartitionNumber, and
        # for the schema, we want the dataset number
        if isinstance(self.bundle, Partition):
            self.d_id=self.bundle.bundle.identity.id_
        else:
            self.d_id=self.bundle.identity.id_

        if not self.d_id:
            raise ValueError("self.bundle.identity.oid not set")
        self._seen_tables = {}
      
        self.table_sequence = 1
        self.col_sequence = 1 

        
    def clean(self):
        from databundles.orm import Table, Column
        s = self.bundle.database.session 
        s.query(Table).delete()
        s.query(Column).delete()        
        
    @property
    def tables(self):
        '''Return a list of tables for this bundle'''
        from databundles.orm import Table
        q = (self.bundle.database.session.query(Table)
                .filter(Table.d_id==self.d_id))

        return q.all()
    
    @classmethod
    def get_table_from_database(cls, db, name_or_id):
        from databundles.orm import Table
        import sqlalchemy.orm.exc
        
        try:
            return (db.session.query(Table)
                    .filter(Table.id_==name_or_id).one())
        except sqlalchemy.orm.exc.NoResultFound:
            return (db.session.query(Table)
                    .filter(Table.name==name_or_id).one())
    
    def table(self, name_or_id):
        '''Return an orm.Table object, from either the id or name'''
        return Schema.get_table_from_database(self.bundle.database, name_or_id)

    def add_table(self, name, **kwargs):
        '''Add a table to the schema'''
        from orm import Table
        from identity import TableNumber, ObjectNumber
           
        name = Table.mangle_name(name)
     
        if name in self._seen_tables:
            raise Exception("Already got "+name)
        
        id_ = str(TableNumber(ObjectNumber.parse(self.d_id), self.table_sequence))
      
        row = Table(id = id_,
                    name=name, 
                    d_id=self.d_id, 
                    sequence_id=self.table_sequence)
        
  
        self.bundle.database.session.add(row)
            
        for key, value in kwargs.items():    
            if key[0] != '_' and key not in ['id','id_', 'd_id','name','sequence_id']:
                setattr(row, key, value)
     
        self._seen_tables[name] = row
     
        self.table_sequence += 1
        self.col_sequence = 1
     
        return row
        
    def add_column(self, table, name,**kwargs):
        '''Add a column to the schema'''
    
        kwargs['sequence_id'] =self.col_sequence
    
        c =  table.add_column(name, **kwargs)
        
        self.col_sequence += 1
        
        return c
        
    @property
    def columns(self):
        '''Return a list of tables for this bundle'''
        from databundles.orm import Column
        return (self.bundle.database.session.query(Column).all())
        
    def get_table_meta(self, name_or_id):
        s = self.bundle.database.session
        from databundles.orm import Table, Column
        
        import sqlalchemy
        from sqlalchemy import MetaData, UniqueConstraint, Index, text
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
        
        try :
            q =  (s.query(Table)
                       .filter(Table.name==name_or_id)
                       .filter(Table.d_id==self.d_id))
          
            table = q.one()
        except:
            # Try it with just the name
            q =  (s.query(Table).filter(Table.name==name_or_id))
             
            table = q.one()
        
        at = SATable(table.name, metadata)
 
        indexes = {}
        constraints = {}
    
                
        for column in table.columns:
            
            kwargs = {}
        
            if column.default is not None:
                try:
                    int(column.default)
                    kwargs['server_default'] = text(str(10))
                except:
                    kwargs['server_default'] = column.default
       
            
            ac = SAColumn(column.name, 
                          translate_type(column), 
                          primary_key = ( column.is_primary_key == 1),
                          **kwargs
                          )

            at.append_column(ac);
            
            # assemble indexes
            if column.indexes and column.indexes.strip():
                for cons in column.indexes.strip().split(','):
                    if cons.strip() not in indexes:
                        indexes[cons.strip()] = []
                    indexes[cons.strip()].append(ac)

            # Assemble constraints
            if column.unique_constraints and column.unique_constraints.strip(): 
                for cons in column.unique_constraints.strip().split(','):
                    
                    if cons.strip() not in constraints:
                        constraints[cons.strip()] = []
                    
                    constraints[cons.strip()].append(column.name)
            
    
        # Append constraints. 
        for constraint, columns in constraints.items():
            at.append_constraint(UniqueConstraint(name=constraint,*columns))
             
        # Add indexes   
        for index, columns in indexes.items():
            print "INDEX",index,columns
            Index(index, unique = True ,*columns)
    
        return metadata, at
        
    def create_tables(self):
        '''Create the defined tables as database tables.'''
        self.bundle.database.commit()
        for t in self.tables:
            if not t.name in self.bundle.database.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(t.name) #@UnusedVariable
                t_meta.create_all(bind=self.bundle.database.engine)
        
       
        
        
        
        
        