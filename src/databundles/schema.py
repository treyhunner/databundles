'''
Created on Jun 23, 2012

@author: eric
'''
def _clean_flag( in_flag):
    
    if in_flag is None or in_flag == '0':
        return False;
    
    return bool(in_flag)

def _clean_int(i):
    
    if isinstance(i, int):
        return i
    elif isinstance(i, basestring):
        if len(i) == 0:
            return None
        
        return int(i.strip())
    elif i is None:
        return None
        raise ValueError("Input must be convertable to an int. got:  ".str(i)) 

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
            try:
                return (db.session.query(Table)
                        .filter(Table.name==name_or_id).one())
            except sqlalchemy.orm.exc.NoResultFound:
                return None
    
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
      
        
        data = { k.replace('d_','',1): v for k,v in kwargs.items() if k.startswith('d_') }
      
        row = Table(id = id_,
                    name=name, 
                    d_id=self.d_id, 
                    sequence_id=self.table_sequence,
                    data=data)
        
        self.bundle.database.session.add(row)
        #
        #
        for key, value in kwargs.items():    
            if key[0] != '_' and key not in ['id','id_', 'd_id','name','sequence_id','table','column']:       
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
        uindexes = {}
        constraints = {}
       
        for column in table.columns:
            
            kwargs = {}
        
            if column.default is not None:
                try:
                    int(column.default)
                    kwargs['server_default'] = text(str(column.default))
                except:
                    kwargs['server_default'] = column.default
          
          
          
            ac = SAColumn(column.name, 
                          translate_type(column), 
                          primary_key = ( column.is_primary_key == 1),
                          **kwargs
                          )

            at.append_column(ac);
            
            # assemble non unique indexes
            if column.indexes and column.indexes.strip():
                for cons in column.indexes.strip().split(','):
                    if cons.strip() not in indexes:
                        indexes[cons.strip()] = []
                    indexes[cons.strip()].append(ac)

            # assemble  unique indexes
            if column.uindexes and column.uindexes.strip():
                for cons in column.uindexes.strip().split(','):
                    if cons.strip() not in uindexes:
                        uindexes[cons.strip()] = []
                    uindexes[cons.strip()].append(ac)


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
            Index(index, unique = False ,*columns)
    
        # Add unique indexes   
        for index, columns in uindexes.items():
            Index(index, unique = True ,*columns)
    
        return metadata, at
        
    def create_tables(self):
        '''Create the defined tables as database tables.'''
        self.bundle.database.commit()
        for t in self.tables:
            if not t.name in self.bundle.database.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(t.name) #@UnusedVariable
                t_meta.create_all(bind=self.bundle.database.engine)
        
    def schema_from_file(self, file_):
        '''Read a CSV file, in a particular format, to generate the schema'''
        from orm import Column
        import csv, re
        
        reader  = csv.DictReader(file_)
    
        #self.bundle.log("Generating schema from file")
       
        t = None

        tm = {
              'TEXT':Column.DATATYPE_TEXT,
              'INTEGER':Column.DATATYPE_INTEGER,
              'REAL':Column.DATATYPE_REAL,
              }

        new_table = True
        last_table = None
        for row in reader:
         
            # If the spreadsheet gets downloaded rom Google Spreadsheets, it is
            # in UTF-8
           
            row = { k:str(v).decode('utf8', 'ignore').encode('ascii','ignore').strip() for k,v in row.items()}
          
            if  row['table'] and row['table'] != last_table:
                new_table = True
                last_table = row['table']

            if new_table and row['table']:
                #print 'Table',row['table']
                t = self.add_table(row['table'], **row)
                new_table = False
              
            # Ensure that the default doesnt get quotes if it is a number. 
            if row.get('default', False):
                try:
                    default = int(row['default'])
                except:
                    default = row['default']
            else:
                default = None
          
            # Build the index and unique constraint values. 
            indexes = [ row['table']+'_'+c for c in row.keys() if (re.match('i\d+', c) and _clean_flag(row[c]))]  
            uindexes = [ row['table']+'_'+c for c in row.keys() if (re.match('ui\d+', c) and _clean_flag(row[c]))]  
            uniques = [ row['table']+'_'+c for c in row.keys() if (re.match('u\d+', c) and  _clean_flag(row[c]))]  
    
            datatype = tm[row['type'].strip()]

            width = _clean_int(row.get('width', None))
            size = _clean_int(row.get('size',None))
    
            if  width and width > 0:
                illegal_value = '9' * width
            else:
                illegal_value = None
   
            
            data = { k.replace('d_','',1): v for k,v in row.items() if k.startswith('d_') }
   
            description = row.get('description','').strip()
            
            
            self.add_column(t,row['column'],
                                   is_primary_key= True if row.get('is_pk', False) else False,
                                   description=description,
                                   datatype=datatype,
                                   unique_constraints = ','.join(uniques),
                                   indexes = ','.join(indexes),
                                   uindexes = ','.join(uindexes),
                                   default = default,
                                   illegal_value = illegal_value,
                                   size = size,
                                   width = width,
                                   data=data
                                   )

        