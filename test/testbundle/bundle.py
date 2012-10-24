'''
Created on Jun 10, 2012

@author: eric
'''

from  databundles.bundle import BuildBundle
import petl.fluent as petl

class Bundle(BuildBundle):
    
    def __init__(self, directory=None):

        self.super_ = super(Bundle, self)
        
        self.super_.__init__(directory)
        
        bg = self.config.build
       
        self.geoheaders_file = self.filesystem.path(bg.geoheaderFile)

    
    def prepare(self):
        from databundles.partition import PartitionIdentity 
        
        self.database.create()
        self.schema.schema_from_file(open(self.geoheaders_file, 'rbU'))
        self.schema.create_tables()
        self.database.commit()
             
        for table in self.schema.tables:       
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.new_partition(pid)   
            partition.create_with_tables(table.name)
                    
    def build(self):
        import random
        from functools import partial
        
        sink = self.database.path
     
        #self.log("Writing random data to: "+ self.database.path)
        fields = [
                  ('tone_id', lambda: None),
                  ('text',partial(random.choice, ['chocolate', 'strawberry', 'vanilla'])),
                  ('integer', partial(random.randint, 0, 500)),
                  ('float', random.random)
                  ]
        f = petl.dummytable(10000,fields) #@UndefinedVariable
        f.tosqlite3(sink, 'tone', create=False) 
        f.tosqlite3(sink, 'ttwo', create=False) 
        f.tosqlite3(sink, 'tthree', create=False)
      
        # Now write random data to each of the pable partitions. 
        
        for partition in self.partitions.all:
            if partition.table.name == 'all':
                continue;
            #self.log("Loading "+partition.name)
            db = partition.database.path
            table_name = partition.table.name
            petl.dummytable(30000,fields).tosqlite3(db, table_name, create=False) #@UndefinedVariable

        