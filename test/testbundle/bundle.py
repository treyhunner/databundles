'''
Created on Jun 10, 2012

@author: eric
'''

from  databundles.bundle import Bundle as Base
import petl.fluent as petl

class Bundle(Base):
    
    def __init__(self, directory):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
    def schemaGenerator(self):
        '''Return schema rows'''
        
        from databundles.orm import Table, Column
        
        yield Table(name='table1')
        yield Table(name='table2')
        yield Column(table_name='table1', name='col1')
        yield Column(table_name='table2', name='col2')
        yield Column(table_name='table3', name='col3')
        yield Table(name='table3')
        yield Table(name='table1', altname='altname')
        
        
        yield Table(name='random')
        yield Column(name='rand1',datatype=Column.DATATYPE_REAL)
        yield Column(name='rand2',datatype=Column.DATATYPE_REAL)
        yield Column(name='rand3',datatype=Column.DATATYPE_REAL)
        yield Column(name='rand4',datatype=Column.DATATYPE_REAL)
        yield Column(name='rand5',datatype=Column.DATATYPE_REAL)

    
    def prepare(self):
        self.schema.generate()
                    
    def build(self):
        
        sink = self.database.path
        self.database.create_table('random')
        petl.randomtable(5, 500).tosqlite3(sink, 'random', create=False) #@UndefinedVariable
        
        self.library.put(self)
        
        bundle = self.library.get(self.identity.id_)
        
        print bundle.identity.name
        
        bundle = self.library.get(self.identity.name)
        
        print bundle.identity.name
        
        first_table = self.schema.tables.pop(0)
        
        print first_table.name, first_table.id_
        
        
        
        