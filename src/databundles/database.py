'''
Created on Jun 10, 2012

@author: eric
'''

import os.path

class ValueInserter(object):
    '''Inserts arrays of values into  database table'''
    def __init__(self, bundle, table, db):
        self.bundle = bundle
        self.table = table
        self.db = db
        
    def insert(self, values):    
        con = self.db.engine.connect()
        ins = self.table.insert().values(values)
        #ins.execution_options(autocommit=False)
        con.execute(ins)
           
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
            self.file_path = None
        
        self.create() # Only creates if does not exist
       
    @property
    def name(self):
        return Database.BUNDLE_DB_NAME

    @property 
    def path(self):
     
        if self.file_path:
            return self.file_path
        else:
            return self.bundle.filesystem.path(
                                self.bundle.filesystem.BUILD_DIR,
                                self.bundle.identity.path+".db")
     
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
            self._engine = create_engine('sqlite:///'+self.path, echo=False) 
            
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
   
    def exists(self):
        return os.path.exists( self.path)
    
    def delete(self):
        
        try :
            os.remove(self.path)
        except:
            pass
        
    def inserter(self, table_name):
      
        if not table_name in self.inspector.get_table_names():
            t_meta, table = self.bundle.schema.get_table_meta(table_name) #@UnusedVariable
            t_meta.create_all(bind=self.engine)
            
            if not table_name in self.inspector.get_table_names():
                raise Exception("Don't have table "+table_name)
        
        return ValueInserter(self.bundle, self.table(table_name), self)
        
    def load_sql(self, sql_file):
        import sqlite3
        conn = sqlite3.connect( self.path)
        sql = open(sql_file).read().strip()
       
        conn.executescript(sql)
        
        conn.commit()
        
    def create(self):
        
        """Create the database from the base SQL"""
        if not self.exists():    
            import databundles     
            script_str = os.path.join(os.path.dirname(databundles.__file__),
                                      Database.PROTO_SQL_FILE)
            self.load_sql(script_str)
                   
    def table(self, table_name): 
        '''Get table metadata from the database''' 
        from sqlalchemy import Table
        metadata = self.metadata
        table = Table(table_name, metadata, autoload=True)
        
        return table
        

class PartitionDb(Database):
    '''a database for a partition file. Partition databases don't have a full schema
    and can load tables as they are referenced, by copying them from the prototype. '''

    def __init__(self, bundle, partition):
        '''''' 
        self.partition = partition
        super(PartitionDb, self).__init__(bundle)  

    @property
    def name(self):
        return self.partition.name
    

    @property
    def path(self):
        return self.bundle.filesystem.path(
                    self.bundle.filesystem.BUILD_DIR,
                    self.partition.path+".db")

    
    def create(self):
        '''Unlike the protodb, create() does not add tables. Tables are copied from the proto on demand''' 
      
class BundleDb(Database):
    '''Represents the database version of a bundle that is installed in a library'''
    def __init__(self, path):
      
        super(BundleDb, self).__init__(None, path)  

