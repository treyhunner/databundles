'''
Created on Jul 13, 2012

@author: eric

Base class bundle for the US Census

'''
from  databundles.bundle import BuildBundle
import os.path  
import yaml
import threading



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
        
        self._urls_cache = None
    
        self._geo_tables = None
    
        self._geo_dim_locks = {} 
    
    def configure_arg_parser(self, argv):
    
        def csv(value):
            return value.split(',')
        
        parser = super(UsCensusBundle, self).configure_arg_parser(argv)
        
        parser.add_argument('-s','--subphase', action='store', default = 'all',  help='Specify a sub-phase')
        
        parser.add_argument('-S','--states', action='store', default = ['all'],  
                             type=csv,  help='Specify a sub-phase')
        
        return parser
    
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
    
    @property
    def geo_tables(self):
        
        if self._geo_tables is None:
            self._geo_tables = []
            m = { t.name:t for t in self.schema.tables }
            
            for table_name in self.geo_table_names():
                table = m[table_name]
                
                if table.data.get('split_table', '') == 'A':
                    self._geo_tables.append(table)
                    
        return self._geo_tables
    
    @property
    def states(self):
        if 'all' in self.run_args.states:
            states = self.urls['geos'].keys()
            states.sort()  
            return states
        else:

            states = [ s for s in self.urls['geos'].keys() if s in self.run_args.states ]
            states.sort()
            return states  
    
    def scrape_urls(self, suffix='_uf1'):
        
        if os.path.exists(self.urls_file):
            self.log("Urls file already exists. Skipping")
            return 
       
        urls = self._scrape_urls(self.config.build.rootUrl,self.states_file, suffix)
   
        with open(self.urls_file, 'w') as f:
            yaml.dump(urls, f,indent=4, default_flow_style=False)
            
        return self.urls

    def make_range_map(self):
        
        if os.path.exists(self.rangemap_file):
            self.log("Re-using range map")
            return

        self.log("Making range map")

        range_map = {}
        
        segment = None
       
        for table in self.schema.tables:
            
            if segment != int(table.data['segment']):
                last_col = 4
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

            #print range_map[segment][table.id_.encode('ascii', 'ignore')]
         
            last_col += col_end
            
            self.ptick('.')

    
        self.ptick('\n')

        with open(self.rangemap_file, 'w')as f:
            yaml.dump(range_map, f,indent=4, default_flow_style=False)  


    def generate_partitions(self):
        from databundles.partition import PartitionIdentity
        #
        # Geo split files
        for table in self.geo_tables:
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)

        # The Fact partitions
        for table in self.fact_tables():
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)

        
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
        from databundles.orm import Column
        
        log = self.log
        tick = self.ptick
        
        commit = False
        
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
                row['commit'] = commit

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
                                           is_foreign_key =True,
                                           commit = commit)
              
            else:

                if row['decimal'] and int(row['decimal']) > 0:
                    dt = Column.DATATYPE_REAL
                else:
                    dt = Column.DATATYPE_INTEGER
           
                self.schema.add_column(t, row['name'],
                            description=row['description'],
                            datatype=dt,
                            data={'segment':row['segment'],'source_col':row['col_pos']},
                            commit=commit)

        tick("\n")
        
        if not commit: # If we don't commit in the library, must do it here. 
            self.database.session.commit()

    def create_split_table_schema(self):
        '''Create the split table schema from  the geoschema_filef. 
        
        The "split" tables are the individual tables, which are split out from 
        the segment files. 
        '''

        from databundles.orm import Column
        with open(self.geoschema_file, 'rbU') as f:
            self.schema.schema_from_file(f)
    
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
    
    def build(self, run_geo_dim_f=None, run_state_tables_f=None,run_fact_db_f=None):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        from multiprocessing import Pool
    
        if self.run_args.subphase in ['test']:
            print self.states
         
        if self.run_args.subphase in ['all','geo-dim']:
            # Split up the state geo files into .csv files, and 
            # create the build/geodim files that will link logrecnos to
            # geo split table records. 
    
            if self.run_args.multi and run_geo_dim_f:
                
                pool = Pool(processes=int(self.run_args.multi))
          
                result = pool.map_async(run_geo_dim_f, enumerate(self.urls['geos'].keys()))
                print result.get()
            else:
                for state in self.states:
                    self.run_geo_dim(state)
             
        if self.run_args.subphase in ['all','join-geo-dim']:   
            self.join_geo_dim()
        
        if self.run_args.subphase in ['all','fact']:   

            # Combine the geodim tables with the  state population tables, and
            # produce .csv files for each of the tables. 
            if self.run_args.multi and run_state_tables_f:
                
                pool = Pool(processes=int(self.run_args.multi))
          
                result = pool.map_async(run_state_tables_f, enumerate(self.urls['geos'].keys()))
                print result.get()
            else:
                for state in self.states:
                    self.log("Building fact tables for {}".format(state))
                    self.run_state_tables(state)
      
      
        if self.run_args.subphase in ['all','load-fact']:  
        # Load all of the fact table tempfiles into the fact table databases
        # and store the databases in the library. 
            if self.run_args.multi and run_fact_db_f:
                pool = Pool(processes=int(self.run_args.multi))
                
                result = pool.map_async(run_fact_db_f, [ (n,table.id_) for n, table in enumerate(self.fact_tables())])
                print result.get()
            else:
                for table in self.fact_tables():
                    self.run_fact_db(table.id_)
              
        return True

    def run_geo_dim(self, state):
        '''Break up the geo files into seperate files, combine them 
        nationally, and store them in temporary CSV files. Creates a set of 
        files for each state, the geodim files, that link the state and logrecnos
        to the primary keys for the geodim records. '''
        
        import time
     
        geo_partitions = self.geo_partition_map() # must come before geo_processors. Creates partitions
        geo_processors = self.geo_processors()
     
        row_hash = {} #@RservedAssignment
        for table_id, cp in geo_processors.items(): #@UnusedVariable
            row_hash[table_id] = {}
     
        row_i = 0
        
      
        marker_f = self.filesystem.build_path('markers',state+"_geo_dim")
        
        if os.path.exists(marker_f):
            self.log("Geo dim exists for {}, skipping".format(state))
            return
        else:
            self.log("Building geo dim for {}".format(state))
       

        record_code_partition = self.get_record_code_partition(geo_processors, geo_partitions)
           
        tf = record_code_partition.database.tempfile(record_code_partition.table, suffix=state)
        if tf.exists:
            tf.delete()
            
        for table_id, cp in geo_processors.items():
            partition = geo_partitions[table_id]
            tf = partition.database.tempfile(partition.table, suffix=state)
            if tf.exists:
                tf.delete()
                
        # Delete any of the files that may still exist. 
                    
        for state, logrecno, geo, segments, geodim in self.generate_rows(state): #@UnusedVariable
           
            if row_i == 0:
                self.log("Starting loop for state: "+state+' ')
                t_start = time.time()
      
            row_i += 1
            
            if row_i % 5000 == 0:
                # Prints the processing rate in 1,000 records per sec.
                self.log("GEO "+state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")
            
            geo_keys = []
       
            # Iterate over all of the geo dimentino tables, taking part of this
            # geo rwo and putting it into the temp file for that geo dim table. 
        
            for table_id, cp in geo_processors.items():
                table,  columns, processors = cp
            
                partition = geo_partitions[table_id]

                if partition != record_code_partition:
                    values=[ f(geo) for f in processors ]
                    values[-1] = self.row_hash(values)
    
                    # Write a record to the dimension table. 
                    #r = self.write_geo_row_db(row_hash, partition, table, columns, values)
                    r = self.write_geo_row_tf(row_hash, partition, table, columns, values, state)
                   
                    geo_keys.append(r) 

            # The first None is for the primary id, the last is for the 
            # hash, which was added automatically to geo_dim tables. 
            values = [None, state, logrecno] + geo_keys + [None]

            tf = record_code_partition.database.tempfile(record_code_partition.table, suffix=state)
            tf.writer.writerow(values)

        # Close all of the tempfiles. 
        for table_id, cp in geo_processors.items():
            partition = geo_partitions[table_id]
            partition.database.tempfile(partition.table, suffix=state).close()
 
        record_code_partition.database.tempfile(record_code_partition.table, suffix=state).close()  
            
        with open(marker_f, 'w') as f:
            f.write(str(time.time()))
        
   
    def run_state_tables(self, state):
        '''Split up the segment files into seperate tables, and link in the
        geo splits table for foreign keys to the geo splits. '''
        import time

        fact_partitions = self.fact_partition_map()
       
        with open(self.rangemap_file, 'r') as f:
            range_map = yaml.load(f) 
        
        # Marker to note when the file is done. 
        marker_f = self.filesystem.build_path('markers',state+"_fact_table")
        
        if os.path.exists(marker_f):
            self.log("state table complete for {}, skipping ".format(state))
            return
        else:
            # If it isn't done, remove it if it exists. 
            for partition in fact_partitions.values():
                tf = partition.database.tempfile(partition.table, suffix=state)
                tf.delete()
  
        row_i = 0

        for state, logrecno, geo, segments, geo_keys in self.generate_rows(state, geodim=True ): #@UnusedVariable
 
            if row_i == 0:
                t_start = time.time()
      
            row_i += 1
            
            if row_i % 10000 == 0:
                # Prints a number representing the processing rate, 
                # in 1,000 records per sec.
                self.log("Fact "+state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")
       
            for seg_number, segment in segments.items():
                for table_id, range in range_map[seg_number].iteritems(): #@ReservedAssignment
                    
                    table = self.get_table_by_table_id(table_id)

                    if not segment:
                        #Some segments have fewer lines than others. 
                        #self.error("Failed to get segment data for {}".format(seg_number))
                        continue
                    
                    seg = segment[range['start']:range['end']]
                    
                    if seg and len(seg) > 0:    
                        # The values can be null for the PCT tables, which don't 
                        # exist for some summary levels.       
                        values =  (geo_keys[0],) + geo_keys[3:-1] + tuple(seg) # Remove the state, logrec  and hash from the geo_key  
                        partition = fact_partitions[table_id]
                        tf = partition.database.tempfile(table, suffix=state)

                        if len(values) != len(tf.header):
                            self.error("Fact Table write error. Value not same length as header")
                            print "Segment: ", segment, state, logrecno
                            print "Header : ",len(tf.header), table.name, tf.header
                            print "Values : ",len(values), values
                            print "Range  : ",seg_number, range
                        
                        tf.writer.writerow(values)
                    
                    else:
                        self.log("{} {} Seg {}, table {}  is empty".format(state, logrecno,  seg_number, table_id))

        #
        # Write the values to tempfiles. 
        # 

        for table_id, partition in fact_partitions.items():
            
            table = self.get_table_by_table_id(table_id)
            tf = partition.database.tempfile(table, suffix=state)
                            
            tf.close()

        with open(marker_f, 'w') as f:
            f.write(str(time.time()))

    def run_fact_db(self, table_id):
        '''Load the fact table for a single table into a database and
        put it in the library. Copies all of the temp files for the state
        into the database. '''
   
        try:
            table = self.schema.table(table_id)
        except:
            self.error("Could not get table for id: "+table_id)
            return
        
        partition = self.fact_partition(table, False)
        
        if self.library.get(partition) and not self.run_args.test:
            self.log("Found in fact table bundle library, skipping.: "+table.name)
            return
        
        partition = self.fact_partition(table, True)
        
        db = partition.database
        
        try:
            db.clean_table(table) # In case we are restarting this run
        except Exception as e:
            self.error("Failed for "+partition.database.path)
            raise e
            
        for state in self.urls['geos'].keys():
            tf = partition.database.tempfile(table, suffix=state)
        
            print "PATH ",tf.path
            if not tf.exists:
                if self.run_args.test:
                    self.log("Missing tempfile, ignoring b/c in test: {}".format(tf.path))
                    return
                else:
                    raise Exception("Fact table tempfile does not exist table={} state={} path={}"
                                    .format(table.name, state, tf.path) )
            else:
                self.log("Loading fact table for {}, {} from  {} ".format(state, table.name, tf.path))
     
            try:
                db.load_tempfile(tf)
                tf.close()
            except Exception as e:
                self.error("Loading fact table failed: {} ".format(e))
                return 

        dest = self.library.put(partition)
        self.log("Install Fact table in library: "+str(dest))

        partition.database.delete()
        
        for state in self.urls['geos'].keys():
            tf = partition.database.tempfile(table, suffix=state)
            tf.delete()

    #############################################
    # Generate rows from multiple files?

    def get_record_code_partition(self, geo_processors=None, geo_partitions=None):
         
        if geo_partitions is None:
            geo_partitions = self.geo_partition_map() # must come before geo_processors. Creates partitions
        
        if  geo_processors is None:
            geo_processors = self.geo_processors()
        
        for table_id, cp in geo_processors.items(): #@UnusedVariable
            partition = geo_partitions[table_id]
            if partition.table.name == 'record_code':
                record_code_partition = partition
                
        return record_code_partition;

    def generate_geodim_rows(self, state):
        '''Generate the rows that were created to link the geo split files with the
        segment tables'''
        
        rcp = self.get_record_code_partition()
      
        sql = "SELECT * FROM record_code WHERE state = :state"
        r = rcp.database.connection.execute(sql, state=state)
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
        
        for table in self.geo_tables:
          
            source_cols = ([c.name for c in table.columns 
                                if not ( c.name.endswith('_id') and not c.is_primary_key)
                                and c.name != 'hash'
                               ])
            
            columns = [c for c in table.columns if c.name in source_cols  ]  
            processors = [CensusTransform(c) for c in columns]
            processors[0] = lambda row : None # Primary key column

            columns += [ table.column('hash')]
            processors += [lambda row : None]
     
            processor_set[table.id_] = (table, columns, processors )         
             
            
        return processor_set
   

        
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
            if False:
                # Having toubles with this causing duplicate hashes
                pass
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
        for table in self.geo_tables:
            partitions[table.id_] = self.geo_partition(table, True)
            
        return partitions
     
    def get_table_by_table_id(self,table_id):  
        '''Get the table definition from the schema'''
        t = self._table_id_cache.get(table_id, False)
        
        if not t:
            t = self.schema.table(table_id)
            self._table_id_cache[table_id] = t

        return t
    
    @property
    def urls(self):

        if self._urls_cache is None:
            with open(self.urls_file, 'r') as f:
                self._urls_cache =  yaml.load(f) 
            
            # In test mode, we only use the first state, to make
            # things run faster. 
            if self.run_args.test:
                x = self._urls_cache['geos'].iteritems().next()
                self._urls_cache['geos'] = dict([x])
 
        return self._urls_cache
        
    
        
    #############
    # Writing Results to Disk        
        
    def row_hash(self, values):
        '''Calculate a hash from a database row, for geo split tables '''  
        import hashlib
        
        m = hashlib.md5()
        for x in values[1:]:  # The first record is the primary key
            m.update(str(x)+'|') # '|' is so 1,23,4 and 12,3,4 aren't the same   
        # Less than 16 to avoid integer overflow issues. Not sure it works. 
        hash = int(m.hexdigest()[:14], 16) # First 8 hex digits = 32 bit @ReservedAssignment
     
        return hash

    def write_geo_row_db(self, hash, partition, table, columns,values): #@ReservedAssignment
        '''Write a geo split table row to the temporary file, but only
        if the hash for the row has never been seen. 
        
        This operation is wrapped in a lock to protect both the hash and file
        '''
        from sqlalchemy.exc import ProgrammingError
        
        with self._geo_dim_locks[table.id_]:
        
            th = hash[table.id_]
                    
            if values[-1] in th:
                return th[values[-1]]
            else:                   
                r = len(th)+1
                th[values[-1]] = r
                values[0] = r
                
                table_meta = partition.database.table(table.name)
                ins = table_meta.insert(values=values)
                try:
                    partition.database.session.execute(ins) 
                except ProgrammingError: 
                    # This usually happens when we try to insert an 8-bit string
                    # The Census files are encoded in IBM850, and these error cases
                    # are converted to unicode. 
                    
                    ov = values
                    values = []
                    for v in ov:
                        if isinstance(v, basestring):
                            values.append(unicode(v.decode("IBM850")))
                        else:
                            values.append(v)
                    ins = table_meta.insert(values=values)
                    partition.database.session.execute(ins) 
            
            return r
        
    def write_geo_row_tf(self, hash, partition, table, columns,values, state): #@ReservedAssignment
        '''Write a geo split table row to the temporary file, but only
        if the hash for the row has never been seen. 
        
        This operation is wrapped in a lock to protect both the hash and file
        '''

        th = hash[table.id_]
                
        if values[-1] in th:
            return th[values[-1]]
        else:                   
            r = len(th)+1
            th[values[-1]] = r
            values[0] = r
            
            tf = partition.database.tempfile(table, suffix=state)
            tf.writer.writerow(values)
        
            return r

    def join_geo_dim(self):
        geo_partitions = self.geo_partition_map() # must come before geo_processors. Creates partitions
        geo_processors = self.geo_processors()


        record_code_partition = self.get_record_code_partition(geo_processors, 
                                                               geo_partitions)
        geo_partitions = self.geo_partition_map() 
        
        hashes = {}
        
        for table_id, partition in  geo_partitions.items():
            
            #if partition.table.name in ['area','record_code','recno', 'block']:
            #    continue
            
            #if partition.table.name != 'urban_type':
            #    continue
                
            if partition.table.name == 'record_code':
                continue
            
            hashes[table_id] = self._join_geo_dim(partition, self.states)
           
        print "DONE"
           
        import time
        time.sleep(60)
           
    def _join_geo_dim(self, partition, states):
        import time

        t_start = time.time()
      
        row_i = 0;
     
        hash_map = {}
        table_name = partition.table.name
        
        self.log("Build hash map for {}".format(partition.table.name))
        
        for state_num,state in enumerate(states):
            tf = partition.database.tempfile(partition.table, suffix=state)
            reader = tf.linereader
            reader.next() # skip the header. 
            line_no = 0
            
            for row in reader:
                row_i += 1
                line_no += 1
                if row_i % 1000000 == 0:
                    self.log("Hash "+str(int( row_i/(time.time()-t_start)))+
                             '/s '+str(row_i/1000)+"K ")
                    
                hash = row[-1]
           
                
                hash_map[hash] = (state, line_no, row_i)

                
            tf.close()
                
        if row_i != len(hash_map):
            self.error("{}: hash map doesn't match number of input rows: {} != {}"
                       .format(partition.table.name, len(hash_map), row_i))

        return hash

        row_i = 0;
        t_start = time.time()
        lines = {}
        state_num = 0
        for state in states:
            state_num += 1
            tf = record_code_partition.database.tempfile(record_code_partition.table, 
                                                         suffix=state)
            
            reader = tf.linereader
            reader.next() # skip the header. 
            lines[state_num] = {}
            for row in reader:
                row_i += 1
                
                if row_i % 100000 == 0:
                    self.log("GEO "+state+" "+str(int( row_i/(time.time()-t_start)))+
                             '/s '+str(row_i/1000)+"K ")
    
                lines[state_num][int(row[2])] = ( int(row[3]),int(row[4]),int(row[5]),int(row[6]),
                int(row[7]),int(row[8]),int(row[9]),int(row[10]),
                int(row[11]),int(row[12]),int(row[13]),int(row[14]))   

        print "Done "
        print len(lines)
        time.sleep(60)

        return 
        for table_id, cp in geo_processors.items():
            partition = geo_partitions[table_id]
            for state in states:
                tf = partition.database.tempfile(partition.table, suffix=state)
                print tf.path


def make_geoid(state, county, tract, block=None, blockgroup=None):
    '''Create a geoid for common blocks. This is not appropriate for
    all summary levels, but it is what is used by census.ire.org
    
    See: 
        http://www.census.gov/rdo/pdf/0GEOID_Construction_for_Matching.pdf
        https://github.com/clarinova/census/blob/master/dataprocessing/load_crosswalk_blocks.py
        
    '''
    
    x = ''.join([
            state.rjust(2, '0'),
            county.rjust(3, '0'),
            tract.rjust(6, '0')
            ])
    
    if block is not None:
        x = x + block.rjust(4, '0')
        
    if blockgroup is not None:
        x = x + blockgroup.rjust(4, '0')
    
    
    

        
    
    
