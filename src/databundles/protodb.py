'''
Created on Jun 10, 2012

@author: eric
'''

import os.path

class ProtoDB():
    '''Represents a prototype database, which stored basic, common configuration and is copied to start a new bundle'''

    PROTO_DB_FILE = 'proto.db'
    PROTO_SQL_FILE = 'configuration.sql' # Stored in the databundles module. 


    def __init__(self, bundle):
        ''''''
          
        self.proto_file=bundle.path(ProtoDB.PROTO_DB_FILE)
      
     
    def metadata(self):
        '''Return an SqlAlchemy MetaData object'''
        
        from sqlalchemy import create_engine,MetaData
        
        engine = create_engine('sqlite:///'+self.proto_file,echo=True)
         
        metadata = MetaData(bind=engine)
       
        return metadata
        

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
        return os.path.exists( self.proto_file)
    
    def delete(self):
        try :
            os.remove(self.proto_file)
        except:
            pass
        
    def create(self):
        
        if not os.path.exists( self.proto_file):
            import databundles
            script_str = os.path.normpath(os.path.dirname(databundles.__file__)+'/'+ProtoDB.PROTO_SQL_FILE)
            
            import sqlite3
            conn = sqlite3.connect( self.proto_file)
            conn.executescript(open(script_str).read().strip())
            conn.commit()
            
        
        