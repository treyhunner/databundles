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
                    table = {'type': 'table', 
                             'name':row['TABLE'],'description':row['TEXT']
                             }
                else:
                    table['universe'] = row['TEXT'].replace('Universe:','').strip()  
            else:
                
                # The whole table will exist in one segment ( file number ) 
                # but the segment id is not included on the same lines ast the
                # table name. 
                if table:
                    # This is yielded  here so we can get the segment number. 
                    table['segment'] = row['SEG'] 
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
                
    def generate_seg_rows(self, seg_number, source):
        '''Generate rows for a segment file. Call this generator with send(), 
        passing in the lexpected logrecno. If the next row does not have that 
        value, return a blank row until the logrecno values match. '''
        import csv
        next_logrecno = None
        with self.filesystem.download(source) as zip_file:
            with self.filesystem.unzip(zip_file) as rf:
                for row in csv.reader(open(rf, 'rbU') ):
                    # The next_logrec bit takes care of a differece in the
                    # segment files -- the PCT tables to not have entries for
                    # tracts, so there are gaps in the logrecno sequence for those files. 
                    while next_logrecno is not None and next_logrecno != row[4]:
                        next_logrecno = (yield seg_number,  [])
             
                    next_logrecno = (yield seg_number,  row)
                 
        return
                    
    def generate_rows(self, state, geodim=False):
        '''A Generator that yelds a tuple that has the logrecno row
        for all of the segment files and the geo file. '''
        import struct
        
        table = self.schema.table('sf1geo')
        header, unpack_str, length = table.get_fixed_unpack() #@UnusedVariable
         
        geo_source = self.urls['geos'][state]
      
        gens = [self.generate_seg_rows(n,source) for n,source in self.urls['tables'][state].items() ]

        geodim_gen = self.generate_geodim_rows(state) if geodim else None
     
        with self.filesystem.download(geo_source) as geo_zip_file:
            with self.filesystem.unzip(geo_zip_file) as grf:
                with open(grf, 'rbU') as geofile:
                    first = True
                    for line in geofile.readlines():
                        geo = struct.unpack(unpack_str, line[:-1])
                         
                        if not geo:
                            raise ValueError("Failed to match regex on line: "+line) 
    
                        segments = {}
                      
                        lrn = geo[6]
                     
                        
                        for g in gens:
                            try:
                                seg_number,  row = g.send(None if first else lrn)
                                segments[seg_number] = row
                                # The logrecno must match up across all files, except
                                # when ( in PCT tables ) there is no entry
                                if len(row) > 5 and row[4] != lrn:
                                    raise Exception("Logrecno mismatch for seg {} : {} != {}"
                                                    .format(seg_number, row[4],lrn))
                            except StopIteration:
                                # Apparently, the StopIteration exception, raised in
                                # a generator function, gets propagated all the way up, 
                                # ending all higher level generators. thanks for nuthin. 
                                break
                    
                        geodim = geodim_gen.next() if geodim_gen is not None else None

                        if geodim and geodim[0] != lrn:
                            raise Exception("Logrecno mismatch for geodim : {} != {}"
                                                    .format(geodim[0],lrn))

                        first = False

                        yield state, segments[1][4], dict(zip(header,geo)), segments, geodim

                    # Check that there are no extra lines. 
                    for g in gens:
                        try:
                            while g.next(): 
                                raise Exception("Should not have extra items left")
                        except StopIteration:
                            pass
        return
               
    def geo_table_names(self):
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
  
 