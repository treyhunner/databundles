'''
Created on Jun 14, 2012

@author: eric
'''
import unittest

class Test(unittest.TestCase):

    def test_basic(self):
        from databundles.config.database import Database, Table, Column
        import uuid
           
        ds = Database()
        
        table_names =  ( "table_"+str(x) for x in range(1,40))
        col_names =  ( "col_"+str(x) for x in range(1,40))
        alt_names =  ( str(uuid.uuid4())[:6] for x in range(1,40) )
      
        t = ds.add_table(Table(name=next(table_names), altname=next(alt_names)))
        t.add_column(Column(name=next(col_names), 
                            altname=next(alt_names),
                            datatype=Column.DATATYPE_TEXT 
                            )
                     )
        t.add_column(Column(name=next(col_names), 
                    altname=next(alt_names),
                    datatype=Column.DATATYPE_INTEGER
                    )
             )
        t.add_column(Column(name=next(col_names), 
                    altname=next(alt_names),
                    datatype=Column.DATATYPE_DATE 
                    )
             )
        t.add_column(Column(name=next(col_names), 
                    altname=next(alt_names),
                    datatype=Column.DATATYPE_TIME
                    )
             )
        
        
        t = ds.add_table(Table(name=next(table_names), altname=next(alt_names)))
        t.add_column(Column(name=next(col_names), 
                            altname=next(alt_names),
                            datatype=Column.DATATYPE_NUMERIC 
                            )
                     )
        t.add_column(Column(name=next(col_names), 
                    altname=next(alt_names),
                    datatype=Column.DATATYPE_TIMESTAMP
                    )
             )
        t.add_column(Column(name=next(col_names), 
                    altname=next(alt_names),
                    datatype=Column.DATATYPE_DATE 
                    )
             )
        t.add_column(Column(name=next(col_names), 
                    altname=next(alt_names),
                    datatype=Column.DATATYPE_TIME
                    )
             )


        ds.propagate_id()
            
        print ds.dump()
        
        return
        
        import pprint
        print pprint.pprint(ds.as_dict())
        
        
        from sqlalchemy import create_engine,MetaData
       
        engine = create_engine('sqlite:////Volumes/Storage/proj/github.com/databundles/tutorial.db',
                               echo=True)
         
        metadata = MetaData(bind=engine)
        
        ds.as_sqlalchemy(metadata)
        
        for t in metadata.sorted_tables:
            print t.name
        
        metadata.create_all()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_basic']
    unittest.main()