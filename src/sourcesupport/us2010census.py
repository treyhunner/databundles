'''
Created on Aug 19, 2012

@author: eric
'''
from  sourcesupport.uscensus import UsCensusBundle
import yaml 

class Us2010CensusBundle(UsCensusBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Us2010CensusBundle, self)
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
        

    def _scrape_urls(self, rootUrl, states_file):
        '''Extract all of the URLS from the Census website and store them. 
        Unline the Us2000 version, this one lists one file per state'''
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
        urls = {}
      
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
                   
                        tick('T')
                        
                        m = re.match('.*/(\w{2})2010.sf1.zip', final_url)

                        if  m:
                            urls[m.group(1)] = str(final_url)
                        else:
                            raise Exception("Regex failed for : "+final_url)
        
        tick('\n')
   
        return urls
    
    def read_packing_list(self):
        '''The packing list is a file, in every state extract directory, 
        that has a section that describes how the tables are packed into segments.
        it appears to be the same for every sttate'''
        import re
    
        # Descend into the first extract directory. The part of the packing list
        # we need is the same for every state. 
        
        urls = yaml.load(file(self.urls_file, 'r'))
        
        pack_list = None
        for state, url in urls.items(): #@UnusedVariable
            with self.filesystem.download(url) as state_file:
                with self.filesystem.unzip_dir(state_file) as files:
                    for f in files:
                        if f.endswith("2010.sf1.prd.packinglist.txt"):
                            pack_list = f
                            break
                    break
        lines = []          
        with open(pack_list) as f:
            for line in f:
                if re.search('^p\d+\|', line):
                    parts = line.strip().split('|')
                    segment, length = parts[1].split(':')
                    lines.append({'table':parts[0],
                                 'segment':segment,
                                 'length':length})

        return lines

    
    def _make_range_map(self, urls_file, segmap_file, schema_lookup ):
        '''Builds a yaml file that links(state,segment,table) to the column ranges
        in the segment file that have data for that table. 
        
        Uses:
            Urls File
            Segmap File
            
        Outputs:
            Rangemap File
        
        '''
    
        urls = yaml.load(file(urls_file, 'r'))
        segmap = yaml.load(file(segmap_file, 'r'))     
    
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
                                'source_col':column.data['source_col']+1, 
                                'name' : table.name.encode('ascii', 'ignore')}
                
            range_map[seg_number] = irm
    
        self.ptick('\n')
        return range_map
    
    
    def generate_schema_rows(self):
        '''This generator yields schema rows from the schema defineition
        files. This one is specific to the files produced by dumpoing the Access97
        shell for the 2010 census '''
        import csv
        
        reader  = csv.DictReader(open(self.headers_file, 'rbU') )
        last_seg = None
        table = None
        for row in reader:
            if not row['TABLE NUMBER']:
                continue
            
            if row['SEGMENT'] and row['SEGMENT'] != last_seg:
                last_seg = row['SEGMENT']
            

            # The first two rows for the table give information about the title
            # and population universe, but don't have any column info. 
            if( not row['FIELD CODE']):
                if  row['FIELD NAME'].startswith('Universe:'):
                    table['universe'] = row['FIELD NAME'].replace('Universe:','').strip()  
                else:
                    table = {'type': 'table', 'name':row['TABLE NUMBER'],'description':row['FIELD NAME']}
            else:
                
                # The whole table will exist in one segment ( file number ) 
                # but the segment id is not included on the same lines ast the
                # table name. 
                if table:
                    table['data'] = {'segment':row['SEGMENT'], 'fact':True}
                    yield table
                    table  = None
                    
                col_pos = int(row['FIELD CODE'][-3:])
                
                yield {
                       'type':'column','name':row['FIELD CODE'], 
                       'description':row['FIELD NAME'].strip(),
                       'segment':int(row['SEGMENT']),
                       'col_pos':col_pos,
                       'decimal':int(row['DECIMAL'])
                       }
 
    
            