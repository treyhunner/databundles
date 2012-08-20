'''
Created on Aug 19, 2012

@author: eric
'''
from  sourcesupport.uscensus import UsCensusBundle
import yaml 

class Us2000CensusBundle(UsCensusBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Us2000CensusBundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.segmap_file =  self.filesystem.path(bg.segMapFile)
        self.headers_file =  self.filesystem.path(bg.headersFile)
        self.geoheaders_file = self.filesystem.path(bg.geoheaderFile)
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.rangemap_file =  self.filesystem.path(bg.rangeMapFile)
        self.urls_file =  self.filesystem.path(bg.urlsFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        
        self._table_id_cache = {}
        self._table_iori_cache = {}
        

    def _scrape_urls(self, rootUrl, states_file, log=lambda msg: True, tick=lambda msg: True):
        '''Extract all of the URLS from the Census website and store them'''
        import urllib
        import urlparse
        import re
        from bs4 import BeautifulSoup
    
        log = self.log
        tick = self.ptick
    
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
    
    def _make_segment_map(self):
        
        import csv
       
        seg_map = {}
        for row in csv.DictReader(open(self.headers_file, 'rbU') ):
            if row['SEG'] and row['TABLE']:
                seg = int(row['SEG'])
                table = row['TABLE'].strip().lower()
                
                if not seg in seg_map:
                    seg_map[seg] = []
                    
                # Want YAML to serialize a list, not a set. 
                if table not in seg_map[seg]:
                    seg_map[seg].append(table)
                
        return seg_map
    
    def _make_range_map(self, schema_lookup ):
        '''Builds a yaml file that links(state,segment,table) to the column ranges
        in the segment file that have data for that table. 
        
        Uses:
            Urls File
            Segmap File
            
        Outputs:
            Rangemap File
        
        '''
    
        urls = yaml.load(file(self.urls_file, 'r'))
        segmap = self._make_segment_map()
    
        range_map = {}
        
        state, segments = urls['tables'].items()[0] #@UnusedVariable
          
        for seg_number,source in segments.items(): #@UnusedVariable
            self.ptick('.')
            
            irm = {}
            for table_name in segmap[seg_number]: 
                table = schema_lookup(table_name)
            
                start =   None
                for column in table.columns:
                    table_id =  table.id_.encode('ascii')        
                    if table_id not in irm:
                        irm[table_id] = []
                      
                    if 'source_col' in column.data and column.data['source_col'] >= 5 and start is None:
                        start = column.data['source_col']
                                 
                irm[table_id] = {
                                'start':start,  
                                'end':column.data['source_col']+1, 
                                'table' : table.name.encode('ascii', 'ignore')}
                
            range_map[seg_number] = irm
    
        self.ptick('\n')
        return range_map
    
    def generate_schema_rows(self):
        '''This generator yields schema rows from the schema defineition
        files. This one is specific to the files produced by dumpoing the Access97
        shell for the 2000 census '''
        import csv
        
        reader  = csv.DictReader(open(self.headers_file, 'rbU') )
        last_seg = None
        table = None
        for row in reader:
            if not row['TABLE']:
                continue
            
            if row['SEG'] and row['SEG'] != last_seg:
                last_seg = row['SEG']
            

            # The first two rows for the table give information about the title
            # and population universe, but don't have any column info. 
            if( not row['FIELDNUM']):
                if  row['TABNO']:
                    table = {'type': 'table', 'name':row['TABLE'],'description':row['TEXT']}
                else:
                    table['universe'] = row['TEXT'].replace('Universe:','').strip()  
            else:
                
                # The whole table will exist in one segment ( file number ) 
                # but the segment id is not included on the same lines ast the
                # table name. 
                if table:
                    table['data'] = {'segment':row['SEG'], 'fact':True}
                    yield table
                    table  = None
                    
                col_pos = int(row['FIELDNUM'][-3:])
                
                yield {
                       'type':'column','name':row['FIELDNUM'], 
                       'description':row['TEXT'].strip(),
                       'segment':int(row['SEG']),
                       'col_pos':col_pos,
                       'decimal':int(row['DECIMAL'])
                       }
 