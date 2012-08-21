'''
Created on Jul 13, 2012

@author: eric

Base class bundle for the US Census

'''
from  databundles.bundle import BuildBundle
import os.path  
import yaml


class UsCensusBundle(BuildBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(UsCensusBundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.segmap_file =  self.filesystem.path(bg.segMapFile)
        self.headers_file =  self.filesystem.path(bg.headersFile)
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.rangemap_file =  self.filesystem.path(bg.rangeMapFile)
        self.urls_file =  self.filesystem.path(bg.urlsFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        
        self._table_id_cache = {}
        self._table_iori_cache = {}
    
    #####################################
    # Peparation
    #####################################
    
    def prepare(self):
        '''Create the prototype database'''

        if not self.database.exists():
            self.database.create()

        self.scrape_urls()
      
        self.create_table_schema()
      
        self.make_range_map()

        self.create_split_table_schema()
    
        self.generate_partitions()
        
        return True
    
    def scrape_urls(self):
        
        if os.path.exists(self.urls_file):
            self.log("Urls file already exists. Skipping")
            return 
       
        urls = self._scrape_urls(self.config.build.rootUrl,self.states_file)
   
        yaml.dump(urls, file(self.urls_file, 'w'),indent=4, default_flow_style=False)
            
        return yaml.load(file(self.urls_file, 'r')) 

    def make_range_map(self):
        
        if os.path.exists(self.rangemap_file):
            self.log("Re-using range map")
            return

        self.log("Making range map")

        range_map = {}
        
        segment = None
       
        for table in self.schema.tables:
            
            if segment != table.data['segment']:
                last_col = 5
         
            segment = int(table.data['segment'])
            
            col_start = min(int(c.data['source_col']) for c in table.columns if c.data.get('source_col', False))
            col_end = max(int(c.data['source_col']) for c in table.columns if c.data.get('source_col', False))
        
            if segment not in range_map:
                range_map[segment] = {}
        
            range_map[segment][table.id_.encode('ascii', 'ignore')] = {
                                'start':last_col + col_start,  
                                'end':last_col + col_end+ 1, 
                                'length': col_end-col_start + 1,
                                'table' : table.name.encode('ascii', 'ignore')}
            
                         
            #print "{:5s} {:4d} {:4d} {:4d} {:4d}".format(table.name,  int(segment), col_end-col_start + 1, 
            #                                        last_col + col_start, last_col + col_end  )
         
            last_col += col_end
            
            self.ptick('.')
         

    
        self.ptick('\n')

        yaml.dump(range_map, file(self.rangemap_file, 'w'),indent=4, default_flow_style=False)  

    def generate_partitions(self):
        from databundles.partition import PartitionIdentity
        #
        # Geo split files
        for table in self.geo_tables():
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)
            else:
                self.log("Already created partition, skipping "+table.name)

        # The Fact partitions
        for table in self.fact_tables():
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)
            else:
                self.log("Already created partition, skipping "+table.name)
        
        # First install the bundle main database into the library
        # so all of the tables will be there for installing the
        # partitions. 
        self.log("Install bundle")
        if self.library.get(self.identity.id_):
            self.log("Found in bundle library, skipping. ")
        else:
            self.library.put(self)
        
        return True

    def create_table_schema(self):
        '''Uses the generate_schema_rows() generator to creeate rows for the fact table
        The geo split table is created in '''
        import csv
        from databundles.orm import Column
        
        log = self.log
        tick = self.ptick
        
        if len(self.schema.tables) > 0 and len(self.schema.columns) > 0:
            log("Reusing schema")
            return True
            
        else:
            log("Regenerating schema. This could be slow ... ")
        
        
        log("Generating main table schemas")
      
        for row in self.generate_schema_rows():
           
            if row['type'] == 'table':
                
                tick(".")
                name = row['name']
                row['data'] = {'segment':row['segment'], 'fact': True}
                del row['segment']
                del row['name']
                t = self.schema.add_table(name, **row )

                # First 5 fields for every record      
                # FILEID           Text (6),  uSF1, USF2, etc. 
                # STUSAB           Text (2),  state/U.S. abbreviation
                # CHARITER         Text (3),  characteristic iteration, a code for race / ethic group
                #                             Prob only applies to SF2. 
                # CIFSN            Text (2),  characteristic iteration file sequence number
                #                             The number of the segment file             
                # LOGRECNO         Text (7),  Logical Record Number

                for fk in self.geo_keys():
                    self.schema.add_column(t, fk,
                                           datatype=Column.DATATYPE_INTEGER, 
                                           is_foreign_key =True,)
              
            else:

                if row['decimal'] and int(row['decimal']) > 0:
                    dt = Column.DATATYPE_REAL
                else:
                    dt = Column.DATATYPE_INTEGER
           
                self.schema.add_column(t, row['name'],
                            description=row['description'],
                            datatype=dt,
                            data={'segment':row['segment'],'source_col':row['col_pos']})
                
        tick("\n")

    def create_split_table_schema(self):
        '''Create the split table schema from  the geoschema_filefile. '''

        from databundles.orm import Column
        self.schema.schema_from_file(open(self.geoschema_file, 'rbU'))

        # Add extra fields to all of the split_tables
        for table in self.schema.tables:
            if not table.data.get('split_table', False):
                continue;
            
            if not table.column('hash', False):
                table.add_column('hash',  datatype=Column.DATATYPE_INTEGER,
                                  uindexes = 'uihash')

    #############################################
    # Build
    #############################################
    
    def build(self, multi_func=None):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
       
        from multiprocessing import Pool

        urls = yaml.load(file(self.urls_file, 'r')) 
        
        n = len(urls['geos'].keys())
        i = 1
        
        for state in urls['geos'].keys():
            self.log("Building Geo state for {}, {} of {}".format(state, i, n))
            self.run_state_geo(state)
            i = i + 1
         
        self.store_geo_splits()
            
        if self.run_args.multi:
            pool = Pool(processes=int(self.run_args.multi))
            result = pool.map_async(multi_func, urls['geos'].keys())
            print result.get()
        else:
            for state in urls['geos'].keys():
                self.log("Building fact tables for {}".format(state))
                self.run_state_tables(state)
          
        return True
       
    def run_state_geo(self, state):
        '''Break up the geo files into seperate files, combine them 
        nationally, and store them in temporary CSV files. Creates a set of 
        files for each state, the geodim files, that link the state and logrecnos
        to the primary keys for the geodim records. '''
        
        import time
     
        row_i = 0
      
        if os.path.exists(self.geo_dim_file(state)):
            self.log("Geo dim exists for {}, skipping".format(state))
            return
        
        self.log("Initializing state: "+state+' ')
    
        geo_processors = self.geo_processors()
      
        geo_partitions = self.geo_partition_map()

        gd_file, geo_dim_writer = self.geo_key_writer(state)
        
        hash = {} #@ReservedAssignment
        counts = {}
        for table_id, cp in geo_processors.items():
            hash[table_id] = {}
            counts[table_id] = 1
        
         
        for state, logrecno, geo, segments, geodim in self.generate_rows(state): #@UnusedVariable
           
            if row_i == 0:
                self.log("Starting loop for state: "+state+' ')
                t_start = time.time()
      
            row_i += 1
            
            if row_i % 10000 == 0:
                # Prints a number representing the processing rate, 
                # in 1,000 records per sec.
                self.log(state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")
             
            geo_keys = []
            
            for table_id, cp in geo_processors.items():
                table, columns, processors = cp
            
                values=[ f(geo) for f in processors ]
                values[-1] = self.row_hash(values)
                      
                partition = geo_partitions[table_id]
                r = self.write_geo_row(hash, partition, table, columns, values)
 
                geo_keys.append(r)    
                
            geo_dim_writer.writerow([logrecno] + geo_keys)
            gd_file.flush()     
        
        gd_file.close()
   
    def run_state_tables(self, state):
        '''Split up the segment files into seperate tables, and link in the
        geo splits table for foreign keys to the geo splits. '''
        import time
        
        fact_partitions = self.fact_partition_map()
        urls = yaml.load(file(self.urls_file, 'r')) 
        range_map = yaml.load(file(self.rangemap_file, 'r')) 
        
        row_i = 0
        
        for state, logrecno, geo, segments, geo_keys in self.generate_rows(state, urls,geodim=True ): #@UnusedVariable
 
            if row_i == 0:
                t_start = time.time()
      
            row_i += 1
            
            if row_i % 10000 == 0:
                # Prints a number representing the processing rate, 
                # in 1,000 records per sec.
                self.log(state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")

            for seg_number, segment in segments.items():
                for table_id, range in range_map[seg_number].iteritems(): #@ReservedAssignment
                    seg = segment[range['start']:range['end']]
                    table = self.get_table_by_table_id(table_id)
                    if len(seg) > 0:    
                        # The values can be null for the PCT tables, which don't 
                        # exist for some summary levels.       
                        values =  geo_keys[1:] + seg # Remove the logrec from the geo_key                                   
                        partition = fact_partitions[table_id]
                        self.write_fact_table(state, partition, table, values)

            for seg_number, segment in segments.items():
                for table_id, range in range_map[seg_number].iteritems(): #@ReservedAssignment
                    table = self.get_table_by_table_id(table_id)
                    tf = partition.database.tempfile(table, suffix=state)
                    tf.close()

    #############################################
    # Generate rows from multiple files. 
    #############################################
    
    def generate_geodim_rows(self, state):
        '''Generate the rows that were created to link the geo split files with the
        segment tables'''
        import csv
    
        file_name = self.geo_dim_file(state)
        with open(file_name, 'r') as f:
            r = csv.reader(f)
            r.next() # Skip the header row
            for row in r:
                yield row
        
        return

    #############
    # Fact Table and Partition Acessors. 
    
    def fact_tables(self):
        for table in self.schema.tables:
            if table.data.get('fact',False):
                yield table
            
    def fact_processors(self):
        '''Generate a complete set of processors for all of the fact tables.
        These processors only deal with the forieng keys to the geo split tables. '''
        from databundles.transform import PassthroughTransform
        
        processor_set = {}
        for table in self.fact_tables():
          
            source_cols = [c.name for c in table.columns if c.is_foreign_key ]
            
            columns = [c for c in table.columns if c.name in source_cols  ]  
            processors = [PassthroughTransform(c) for c in columns]
     
            processor_set[table.id_] = (table, columns, processors )  
       
        return processor_set   
    
    def fact_partition(self, table, init=False):
        '''Called in geo_partition_map to fetch, and create, the partition for a
        table ''' 
        from databundles.partition import PartitionIdentity
    
        pid = PartitionIdentity(self.identity, table=table.name)
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        if not partition:
            raise Exception("Failed to get partition: "+str(pid.name))
        
        if init and not partition.database.exists():
            partition.create_with_tables(table.name)
            
        return partition
    
    def fact_partition_map(self):
        '''Create a map from table id to partition for the geo split table'''
        partitions = {}
        for table in self.fact_tables():
            partitions[table.id_] = self.fact_partition(table)
 
 
        return partitions 
          
    #############
    # Geo Table and Partition Acessors.    
     
    def geo_processors(self):
        '''Generate a complete set of geo processors for all of the split tables'''
        from databundles.transform import  CensusTransform
        from collections import  OrderedDict
        
        processor_set = OrderedDict()
        
        for table in self.geo_tables():
          
            source_cols = ([c.name for c in table.columns 
                                if not ( c.name.endswith('_id') and not c.is_primary_key)
                                and c.name != 'hash'
                               ])
            
            columns = [c for c in table.columns if c.name in source_cols  ]  
            processors = [CensusTransform(c) for c in columns]
            processors[0] = lambda row : None # Primary key column
            
            if table.name != 'record_code':
                columns += [ table.column('hash')]
                processors += [lambda row : None]
     
            processor_set[table.id_] = (table, columns, processors )         
             
            
        return processor_set
   

    def geo_tables(self):
        
        m = { t.name:t for t in self.schema.tables }
        
        for table_name in self.geo_table_names():
            table = m[table_name]
            
            if table.data.get('split_table', '') == 'A':
                yield table
        
    def geo_keys(self):
        return  [ t+'_id' for t in self.geo_table_names()]
    
    def geo_key_columns(self):
        ''' '''

        column_sets = {}
        for table in self.fact_tables():
          
            source_cols = [c.name for c in table.columns if c.is_foreign_key ]
         
            column_sets[table.id_] = (table, source_cols)  
       
        return column_sets
        
    def geo_partition(self, table, init=False):
        '''Called in geo_partition_map to fetch, and create, the partition for a
        table ''' 
        from databundles.partition import PartitionIdentity
        from databundles.database import  insert_or_ignore
        
        pid = PartitionIdentity(self.identity, table=table.name)
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        if not partition:
            raise Exception("Failed to get partition: "+str(pid.name))
        
        if init and not partition.database.exists():
            partition.create_with_tables(table.name)
            
            # Ensure that the first record is the one with all of the null values
            vals = [c.default for c in table.columns]
            vals[-1] = self.row_hash(vals)
            vals[0] = 0;
            
            ins = insert_or_ignore(table.name, table.columns)
            db = partition.database
            db.dbapi_cursor.execute(ins, vals)
            db.dbapi_connection.commit()
            db.dbapi_close()
            
        return partition
   
    def geo_partition_map(self):
        '''Create a map from table id to partition for the geo split table'''
        partitions = {}
        for table in self.geo_tables():
            partitions[table.id_] = self.geo_partition(table)
            
        return partitions
     
    def get_table_by_table_id(self,table_id):  
        '''Get the table definition from the schema'''
        t = self._table_id_cache.get(table_id, False)
        
        if not t:
            t = self.schema.table(table_id)
            self._table_id_cache[table_id] = t

        return t
        
    #############
    # Writing Results to Disk        
        
    def row_hash(self, values):
        '''Calculate a hash from a database row, for geo split tables '''  
        import hashlib
        
        m = hashlib.md5()
        for x in values[1:]:  # The first record is the primary key
            m.update(str(x))   
        hash = int(m.hexdigest()[:15], 16) # First 8 hex digits = 32 bit @ReservedAssignment
     
        return hash
          
    def geo_dim_file(self,state):
        return self.filesystem.build_path('geodim',state)
      
    def geo_key_writer(self, state):
        import csv
        f = self.geo_dim_file(state)
        
        gk_file = open(f, 'wb')
        writer = csv.writer(gk_file)
        
        # Write the header row
        writer.writerow(['logrecno'] + self.geo_keys())
        
        return gk_file, writer

    def write_geo_row(self, hash, partition, table, columns,values): #@ReservedAssignment
        '''Write a geo split table row to the temporary file, but only
        if the hash for the row has never been seen. '''
        th = hash[table.id_]
                
        if values[-1] in th:
            return th[values[-1]]
        else:                   
            r = len(th)+1
            th[values[-1]] = r
            values[0] = r
            tf = partition.database.tempfile(table)
            tf.writer.writerow(values)
            tf.file.flush()
        
        return r
    
    def write_fact_table(self, state, partition, table,  values):
        tf = partition.database.tempfile(table, suffix=state)
        tf = tf.writer.writerow(values)

    def store_geo_splits(self):
        '''Copy all of the geo split CSV files -- the tempfiles -- into
        database partition and store them in the library '''
        
        for table in self.geo_tables():
            partition = self.geo_partition(table, init=True)
            db = partition.database
            if not db.tempfile(table).exists:
                self.log("Geosplit tempfile doe not exist "+table.name)
            else:
                self.log("Loading geo split: "+table.name)
                continue
            
            db.load_tempfile(table)
            
            dest = self.library.put(partition)
            self.log("Install in library: "+dest)
            
            db.tempfile(table).delete()
            
            partition.database.delete()

