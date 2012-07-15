'''
Created on Jun 10, 2012

@author: eric
'''

from  databundles.bundle import BuildBundle
import petl.fluent as petl

class Bundle(BuildBundle):
    
    def __init__(self, directory):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
    
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
        
        
        
        