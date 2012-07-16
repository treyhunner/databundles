'''
Created on Jul 13, 2012

@author: eric

Base class bundle for the US Census

'''
from  databundles.bundle import BuildBundle

import os.path  

class UsCensusBundle(BuildBundle):
    '''Base class for  bundles that process the US census
    
    '''

    def __init__(self,directory=None):
        self.super_ = super(UsCensusBundle, self)
        self.super_.__init__(directory)
        
        self._source_to_partition = None
        
        bg = self.config.build
        self.urls_file =  self.filesystem.path(bg.urlsFile)
        self.segmap_file =  self.filesystem.path(bg.segMapFile)
        self.rangemap_file =  self.filesystem.path(bg.rangeMapFile)
        self.geoheaders_file = self.filesystem.path(bg.geoheaderFile)
        self.headers_file =  self.filesystem.path(bg.headersFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        self.partitions_file =  self.filesystem.path(bg.partitionsFile)
        
    def geoSchemaGenerator(self):
        from databundles.orm import Table, Column
        import csv
        
        self.log("Create GEO schema")
        yield Table(name='sf1geo',description='Geo header')
      
        reader  = csv.DictReader(open(self.geoheaders_file, 'rbU') )
        types = {'TEXT':Column.DATATYPE_TEXT,
                 'INTEGER':Column.DATATYPE_REAL}
        for row in reader: 
            yield Column(name=row['column'],datatype=types[row['datatype'].strip()])
    
    def tableSchemaGenerator(self):
        '''Return schema rows from the  columns.csv file'''
        from databundles.orm import Table, Column
        import csv
    

        self.log("Generating main table schemas")
        self.log("T = Table, C = Column, 5 = Five geo columns")
    
        reader  = csv.DictReader(open(self.headers_file, 'rbU') )
      
        last_seg = None
        source_col = 5 # Offset for  common columns

        for row in reader:
            if not row['TABLE']:
                continue
      
            # Keep track of the column number for each segment file, resetting it when
            # the seg value changes. 
            if row['SEG'] and row['SEG'] != last_seg:
                    source_col = 5
                    last_seg = row['SEG']
      
            # The first two rows for the table give information about the title
            # and population universe, but don't have any column info. 
            if( not row['FIELDNUM']):
                if  row['TABNO']:
                    table = Table(name=row['TABLE'],description=row['TEXT'])
                else:
                    table.universe = row['TEXT'].replace('Universe:','').strip()  
            else:
                
                # The whole table will exist in one segment ( file number ) 
                # but the segment id is not included on the same lines ast the
                # table name. 
                if table:
                    table.data = {'segment':row['SEG']}
                    self.ptick("T")
                    yield table
                     
                    # First 5 fields for every record      
                    # FILEID           Text (6),  uSF1, USF2, etc. 
                    # STUSAB           Text (2),  state/U.S. abbreviation
                    # CHARITER         Text (3),  characteristic iteration, a code for race / ethic group
                    #                             Prob only applies to SF2. 
                    # CIFSN            Text (2),  characteristic iteration file sequence number
                    #                             The number of the segment file             
                    # LOGRECNO         Text (7),  Logical Record Number
              
                    tn = row['TABLE']
                    dt = Column.DATATYPE_INTEGER
                    seg = row['SEG']
                    
                    self.ptick("5")
                    yield Column(name='FILEID',table_name=tn,datatype=dt,
                                 data={'source_col':0,'segment':seg})
                    yield Column(name='STUSAB',table_name=tn,datatype=dt,
                                 data={'source_col':1,'segment':seg})
                    yield Column(name='CHARITER',table_name=tn,datatype=dt,
                                 data={'source_col':2,'segment':seg})
                    yield Column(name='CIFSN',table_name=tn,datatype=dt,
                                 data={'source_col':3,'segment':seg})
                    yield Column(name='LOGRECNO',table_name=tn,datatype=dt,
                                 data={'source_col':4,'segment':seg})
                    
                    table = None

                if row['DECIMAL'] and int(row['DECIMAL']) > 0:
                    dt = Column.DATATYPE_REAL
                else:
                    dt = Column.DATATYPE_INTEGER
                
                self.ptick("C")
                yield Column(name=row['FIELDNUM'],table_name=row['TABLE'],
                             description=row['TEXT'].strip(),
                              datatype=dt,data={'segment':int(row['SEG']),
                                                'source_col':source_col}   )
                
                source_col += 1
                
  
    def scrape_files(self, states_file=None, urls_file=None):
        '''Extract all of the URLS from the Census website and store them'''
        import urllib
        import urlparse
        import yaml
        import re
        from bs4 import BeautifulSoup
     
        if states_file is None:
            states_file = self.states_file
        
        if urls_file is None:
            urls_file = self.urls_file
     
        if os.path.exists(urls_file):
            self.log("Urls file already exists. Skipping")
            return        
   
        # Load in a list of states, so we know which links to follow
        with open(states_file) as f:
            states = map(lambda s: s.strip(),f.readlines())
        
        # Root URL for downloading files. 
        url = self.config.group('build').get('rootUrl')
       
        doc = urllib.urlretrieve(url)
        
        self.log('Getting URLS from '+url)
        # Get all of the links
        self.log('S = state, T = segment table, g = geo')
        tables = {}
        geos = {}
        for link in BeautifulSoup(open(doc[0])).find_all('a'):
            self.ptick('S')
            if not link.get('href') or not link.string or not link.contents:
                continue;# Didn't get a sensible link
            # Only descend into links that name a state
            state = link.get('href').strip('/')
            if link.string and link.contents[0] and state in states :
                stateUrl = urlparse.urljoin(url, link.get('href'))
                stateIndex = urllib.urlretrieve(stateUrl)
                # Get all of the zip files in the directory
                for link in  BeautifulSoup(open(stateIndex[0])).find_all('a'):
                    
                    if link.get('href') and  '.zip' in link.get('href'):
                        final_url = urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')
                    
                        if 'geo_uf1' in final_url:
                            self.ptick('g')
                            state = re.match('.*/(\w{2})geo_uf1', final_url).group(1)
                            geos[state] = final_url
                        else:
                            self.ptick('T')
                            m = re.match('.*/(\w{2})(\d{5})_uf1', final_url)
                            state,segment = m.groups()
                            segment = int(segment.lstrip('0'))
                            if not state in tables:
                                tables[state] = {}
                                
                            tables[state][segment] = final_url

        yaml.dump({'tables':tables,'geos':geos}, 
                  file(urls_file, 'w'),indent=4, default_flow_style=False)
           
    def build_range_map(self):
        '''Builds a yaml file that links(state,segment,table) to the column ranges
        in the segment file that have data for that table. 
        
        Uses:
            Urls File
            Segmap File
            
        Outputs:
            Rangemap File
        
        '''
        import yaml 
     
 
        urls = yaml.load(file(self.urls_file, 'r'))
        segmap = yaml.load(file(self.segmap_file, 'r'))     

        if os.path.exists(self.rangemap_file):
            self.log("Rangemap exists, skipping. ( But loading from file can be slow. )")
            return yaml.load(file(self.rangemap_file, 'r'))     
        else:
            self.log("Building rangemap")
         
        range_map = {}
        for state,segments in urls['tables'].items():
            self.ptick(state)
            range_map[state] = {}
            for seg_number,source in segments.items(): #@UnusedVariable
                self.ptick('.')
                
                irm = {}
                for table_name in segmap[seg_number]: 
                    table = self.schema.table(table_name)
                
                    start =   None
                    for column in table.columns:
                        table_id =  table.id_.encode('ascii')        
                        if table_id not in irm:
                            irm[table_id] = []
                          
                        if column.data['source_col'] >= 5 and start is None:
                            start = column.data['source_col']
                                     
                    irm[table_id] = {
                                    'start':start,  
                                    'source_col':column.data['source_col']+1, 
                                    'table' : table.name.encode('ascii', 'ignore')}
                    
                range_map[state][seg_number] = irm
    
        yaml.dump(range_map, 
                  file(self.rangemap_file, 'w'),indent=4, default_flow_style=False) 
        
        return range_map

    def make_segment_map(self):
        
        import csv
        import yaml

        if os.path.exists(self.segmap_file):
            self.log("Re-using segment map")
            return;
   
        self.log("Making segment map")
   
        map_ = {}
        for row in csv.DictReader(open(self.headers_file, 'rbU') ):
            if row['SEG'] and row['TABLE']:
                seg = int(row['SEG'])
                table = row['TABLE'].strip().lower()
                
                if not seg in map_:
                    map_[seg] = []
                    
                # Want YAML to serialize a list, not a set. 
                if table not in map_[seg]:
                    map_[seg].append(table)
                
        yaml.dump(map_, 
                  file(self.segmap_file, 'w'),indent=4, default_flow_style=False)  

    def build_schema(self):
        if len(self.schema.tables) > 0 and len(self.schema.columns) > 0:
            self.log("Reusing schema")
            
        else:
            self.log("Regenerating schema. This could be slow ... ")
            self.schema.generate()
           
    def get_geo_regex(self):
            '''Read the definition for the fixed positioins of the fields in the geo file and
            construct a regular expresstion to parse the lines.'''
            import csv, re
                    
            reader  = csv.DictReader(open(self.geoheaders_file, 'rbU') )
            pos = 0;
            regex = ''
            header = []
            for row in reader:
                #start = int(row['start']) - 1
                pos += int(row['length'])
            
                regex += "(.{{{}}})".format(row['length'])
                header.append(row['column'])
           
            return header, re.compile(regex)  
        
        
    def build_partitions(self, range_map):
        from databundles.partition import PartitionIdentity
        
        if self.partitions.count > 0:
            self.log("Already have partitions, skipping build_partitions")
            return 
        
        if os.path.exists(self.partitions_file):
            self.log("No partitions, but have partitions sql file. Loading")
            self.database.load_sql(self.partitions_file)
            if self.partitions.count > 0:
                self.log("Now we have partitions. Carry on. ")
                return 
        
        self.log("Building partitions")
        count = len(range_map.items())
        for state, sub1 in range_map.iteritems():
            self.ptick(state+' '+str(count))
            count = count - 1
            for seg_number, sub2 in sub1.iteritems(): #@UnusedVariable
                self.ptick('s')
                for table, data in sub2.iteritems(): # table is the id, not the name @UnusedVariable
                    self.ptick('t')
                    pid = PartitionIdentity(self.identity, table=data['table'], space=state)
                  
                    # Will not re-create the partition if it already exists. 
                    self.partitions.new_partition(pid)
                   
        self.database.session.commit()
        import subprocess
        output = subprocess.check_output(
                    ['sqlite3',self.database.path, '.dump partitions'])
        
        
        with open(self.partitions_file, 'wb') as f:
            f.write("DROP TABLE PARTITIONS;\n"+output)
            
