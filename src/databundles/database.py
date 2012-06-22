'''
Created on Jun 10, 2012

@author: eric
'''

import os.path

class Database(object):
    '''Represents a Sqlite database'''

    BUNDLE_DB_NAME = 'bundle'
    PROTO_SQL_FILE = 'support/configuration.sql' # Stored in the databundles module. 

    def __init__(self, bundle, file_path=None):    
        self.bundle = bundle 
        
        self._engine = None
        self._session = None
        
        if file_path:
            self.file_path = file_path
        else:
            self.file_path =  self.bundle.filesystem.path(bundle.filesystem.BUILD_DIR,self.name+".db")
        
        self.create() # Only creates if does not exist
        
       
    @property
    def name(self):
        return Database.BUNDLE_DB_NAME

    @property 
    def path(self):
        return self.file_path
     
    @property
    def metadata(self):
        '''Return an SqlAlchemy MetaData object, bound to the engine'''
        
        from sqlalchemy import MetaData   
        metadata = MetaData(bind=self.engine)
       
        return metadata
    
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        
        if not self._engine:
            self._engine = create_engine('sqlite:///'+self.path) 
            
        return self._engine

    @property
    def inspector(self):
        from sqlalchemy.engine.reflection import Inspector

        return Inspector.from_engine(self.engine)

    @property
    def session(self):
        '''Return a SqlAlchemy session'''
        from sqlalchemy.orm import sessionmaker
        
        if not self._session:    
            Session = sessionmaker(bind=self.engine)
            self._session = Session()
            
        return self._session

    def add_schema(self, ds):
        '''Add schema from a databundles.config.database object'''
 
        import sqlalchemy
 
        from  databundles.config.database import Column
     
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

        metadata = self.metadata()
   
        for table in ds.tables:
            # Create the ID column
            at = sqlalchemy.Table(table.name, metadata,sqlalchemy.Column('id',sqlalchemy.Integer, primary_key = True))
 
            for column in table.columns:
                ac = sqlalchemy.Column(column.name, translate_type(column), primary_key = False)
    
                at.append_column(ac);
        
        metadata.create_all()
        
    def exists(self):
        return os.path.exists( self.path)
    
    def delete(self):
        try :
            os.remove(self.path)
        except:
            pass
        
    def load_sql(self, sql_file):
        import sqlite3
        conn = sqlite3.connect( self.path)
        conn.executescript(open(sql_file).read().strip())
        
        conn.commit()
        
    def create(self):
        """Create the database from the base SQL"""
        if not self.exists():
            import databundles
            from databundles.config.orm import Dataset
            script_str = os.path.join(os.path.dirname(databundles.__file__),
                                      Database.PROTO_SQL_FILE)
            self.load_sql(script_str)
               
           
    def copy_table(self,table_name):
        '''Copy a table from the prototype database to the current database, if it does not exist'''
        
        print table_name in self.inspector.get_table_names()
        
      
    def table(self, table_name):  
        from sqlalchemy import Table
        metadata = self.metadata
        table = Table(table_name, metadata, autoload=True)
        
        return table
        

class PartitionDB(Database):
    '''a database for a partition file. Partition databases don't have a full schema
    and can load tables as they are referenced, by copying them from the prototype. '''

    def __init__(self, partition):
        ''''''
        super(PartitionDB, self).__init__(partition.bundle)  
    
    def table(self, table_name):
        '''Return a table object, copying it from the protodb to thos one if it does not exist'''
        
        from sqlalchemy import Table
        metadata = self.metadata
        table = Table(table_name, metadata, autoload=True)
        
        return table
    
    @property
    def name(self):
        return Database.BUNDLE_DB_NAME
    
    def create(self):
        '''Unlike the protodb, create() does not add tables. Tables are copied from the proto on demand''' 
      
    
