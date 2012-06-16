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
        
        table_names =  ( "table_"+str(x) for x in range(1,10))
        col_names =  ( "col_"+str(x) for x in range(1,10))
        alt_names =  ( str(uuid.uuid4())[:6] for x in range(1,10) )
      
        t = ds.add_table(Table(name=next(table_names), altname=next(alt_names)))
        t.add_column(Column(name=next(col_names), 
                            altname=next(alt_names),
                            datatype=Column.DATATYPE_TEXT 
                            )
                     )
        
        t = ds.add_table(Table(name=next(table_names), altname=next(alt_names)))
        t.add_column(Column(name=next(col_names), 
                    altname=next(alt_names),
                    datatype=Column.DATATYPE_TEXT 
                    )
             )
        
        t = ds.add_table(Table(name=next(table_names), altname=next(alt_names)))
        t.add_column(Column(name=next(col_names), 
                    altname=next(alt_names),
                    datatype=Column.DATATYPE_TEXT 
                    )
             )

        ds.propagate_id()
            
        print ds.dump()
        
        import pprint
        print pprint.pprint(ds.as_dict())
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_basic']
    unittest.main()