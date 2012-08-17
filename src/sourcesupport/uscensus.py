'''
Created on Jul 13, 2012

@author: eric

Base class bundle for the US Census

'''
from  databundles.bundle import BuildBundle

import os.path  

def scrape_files(rootUrl, states_file, log=lambda msg: True, tick=lambda msg: True):
    '''Extract all of the URLS from the Census website and store them'''
    import urllib
    import urlparse
    import re
    from bs4 import BeautifulSoup

    # Load in a list of states, so we know which links to follow
    with open(states_file) as f:
        states = map(lambda s: s.strip(),f.readlines())
    
    
    # Root URL for downloading files. 
   
    doc = urllib.urlretrieve(rootUrl)
    
    log('Getting URLS from '+rootUrl)
    # Get all of the links
    log('S = state, T = segment table, g = geo')
    tables = {}
    geos = {}
   
    for link in BeautifulSoup(open(doc[0])).find_all('a'):
        tick('S')
        if not link.get('href') or not link.string or not link.contents:
            continue;# Didn't get a sensible link
        # Only descend into links that name a state
        state = link.get('href').strip('/')
      
        if link.string and link.contents[0] and state in states :
            stateUrl = urlparse.urljoin(rootUrl, link.get('href'))
            stateIndex = urllib.urlretrieve(stateUrl)
            # Get all of the zip files in the directory
            
            for link in  BeautifulSoup(open(stateIndex[0])).find_all('a'):
                
                if link.get('href') and  '.zip' in link.get('href'):
                    final_url = urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')
                   
                    if 'geo_uf1' in final_url:
                        tick('g')
                        state = re.match('.*/(\w{2})geo_uf1', final_url).group(1)
                        geos[state] = final_url
                    else:
                        tick('T')
                        m = re.match('.*/(\w{2})(\d{5})_uf1', final_url)
                        state,segment = m.groups()
                        segment = int(segment.lstrip('0'))
                        if not state in tables:
                            tables[state] = {}
                            
                        tables[state][segment] = final_url
    
    return {'tables':tables,'geos':geos}
        

def make_segment_map(headers_file, log=lambda msg: True, tick=lambda msg: True):
    
    import csv
    import yaml

    map_ = {}
    for row in csv.DictReader(open(headers_file, 'rbU') ):
        if row['SEG'] and row['TABLE']:
            seg = int(row['SEG'])
            table = row['TABLE'].strip().lower()
            
            if not seg in map_:
                map_[seg] = []
                
            # Want YAML to serialize a list, not a set. 
            if table not in map_[seg]:
                map_[seg].append(table)
            
    return map_
    
def make_range_map(urls_file, segmap_file, schema_lookup, log=lambda msg: True, tick=lambda msg: True):
    '''Builds a yaml file that links(state,segment,table) to the column ranges
    in the segment file that have data for that table. 
    
    Uses:
        Urls File
        Segmap File
        
    Outputs:
        Rangemap File
    
    '''
    import yaml 

    urls = yaml.load(file(urls_file, 'r'))
    segmap = yaml.load(file(segmap_file, 'r'))     

    range_map = {}
    
    
    state, segments = urls['tables'].items()[0]
      
    for seg_number,source in segments.items(): #@UnusedVariable
        tick('.')
        
        irm = {}
        for table_name in segmap[seg_number]: 
            table = schema_lookup(table_name)
        
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
            
        range_map[seg_number] = irm

    tick('\n')
    return range_map

def geo_tables():
    return (['recno',
             'area',
             'block',
             'cons_city',
             'county',
             'leg_district',
             'metro_type',
             'place',
             'schools',
             'spec_area',
             'state', 
             'urban_type',     
             ]
            )
    
def geo_keys():
    return  [ t+'_id' for t in geo_tables()]

def generate_table_schema(headers_file, schema,  log=lambda msg: True, tick=lambda msg: True):
    '''Return schema rows from the  columns.csv file'''
    from databundles.orm import Column
    import csv
    
    if len(schema.tables) > 0 and len(schema.columns) > 0:
        log("Reusing schema")
        return True
        
    else:
        log("Regenerating schema. This could be slow ... ")
    
    
    log("Generating main table schemas")
    log("T = Table, C = Column, X = extra columns")
    
    reader  = csv.DictReader(open(headers_file, 'rbU') )
    
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
                table = {'name':row['TABLE'],'description':row['TEXT']}
            else:
                table['universe'] = row['TEXT'].replace('Universe:','').strip()  
        else:
            
            # The whole table will exist in one segment ( file number ) 
            # but the segment id is not included on the same lines ast the
            # table name. 
            if table:
                table['data'] = {'segment':row['SEG'], 'fact':True}
                tick("T")
                name = table['name']
                del table['name']
                t = schema.add_table(name, **table)
                 
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
                
                tick("X")
                #schema.add_column(t, 'FILEID',table_name=tn,datatype=dt,
                #             data={'source_col':0,'segment':seg})
                #schema.add_column(t, 'STUSAB',table_name=tn,datatype=dt,
                #             data={'source_col':1,'segment':seg})
                #schema.add_column(t, 'CHARITER',table_name=tn,datatype=dt,
                #             data={'source_col':2,'segment':seg})
                #schema.add_column(t, 'CIFSN',table_name=tn,datatype=dt,
                #             data={'source_col':3,'segment':seg})
                #schema.add_column(t, 'LOGRECNO',table_name=tn,datatype=dt,
                #             data={'source_col':4,'segment':seg})
                
                
                for fk in geo_keys():
                    schema.add_column(t, fk,table_name=tn,datatype=dt, is_foreign_key =True)
                
                table = None
    
            if row['DECIMAL'] and int(row['DECIMAL']) > 0:
                dt = Column.DATATYPE_REAL
            else:
                dt = Column.DATATYPE_INTEGER
            
            tick("C")
            schema.add_column(t, row['FIELDNUM'],table_name=row['TABLE'],
                         description=row['TEXT'].strip(),
                          datatype=dt,data={'segment':int(row['SEG']),
                                            'source_col':source_col}   )
            
            source_col += 1
    tick("\n")



class UsCensusBundle(BuildBundle):
    '''Base class for  bundles that process the US census
    
    '''

    def __init__(self,directory=None):
        self.super_ = super(UsCensusBundle, self)
        self.super_.__init__(directory)
        
        self._source_to_partition = None
        
        bg = self.config.build
        

        
       
        
        self.states_file =  self.filesystem.path(bg.statesFile)
        self.partitions_file =  self.filesystem.path(bg.partitionsFile)
        
    def clean(self):
        
        bg = self.config.build
        
        files = [
                    self.filesystem.path(bg.urlsFile),
                    self.filesystem.path(bg.segMapFile),
                    self.filesystem.path(bg.rangeMapFile),
                    self.filesystem.path(bg.partitionsFile)
                 ]
        
        for f in files:
            if os.path.exists(f):
                os.remove(f)
                
        super(UsCensusBundle, self).clean()
        

 
   

        
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
            
