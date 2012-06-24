'''
Created on Jun 10, 2012

@author: eric
'''

from  databundles.bundle import Bundle as Base


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
        
            
        