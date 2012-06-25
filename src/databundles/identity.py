'''
Created on Jun 23, 2012

@author: eric
'''

import exceptions

class Identity(object):
    '''Accessor for identity information. Tied to the dataset record in the
    bundle database. '''
    
    from databundles.properties import DbRowProperty
    
    id_ = DbRowProperty("id_",None,ascii=True)
    source = DbRowProperty("source",None)
    dataset = DbRowProperty("dataset",None)
    subset = DbRowProperty("subset",None)
    variation = DbRowProperty("variation",None)
    creator = DbRowProperty("creator",None)
    revision = DbRowProperty("revision",None)
    
    def __init__(self, bundle):
        self.bundle = bundle

    @property
    def row(self):
        '''Return the dataset row object for this bundle'''
        from databundles.orm import Dataset
        session = self.bundle.database.session
        return session.query(Dataset).first()
        
    @property
    def creatorcode(self):
        import hashlib
        # Create the creator code if it was not specified. 
        return hashlib.sha1(self.creator).hexdigest()[0:4]
       
    @property
    def name(self):
        return  '-'.join(self.name_parts())
    
    def name_parts(self):
        """Return the parts of the name as a list, for additional processing. """
        name_parts = [];
     
        try: 
            name_parts.append(self.source)
        except:
            raise exceptions.ConfigurationError('Missing identity.source')
  
        try: 
            name_parts.append(self.dataset)
        except:
            raise exceptions.ConfigurationError('Missing identity.dataset')  
        
        try: 
            name_parts.append(self.subset)
        except:
            pass
        
        try: 
            name_parts.append(self.variation)
        except:
            pass
        
        try: 
            name_parts.append(self.creatorcode)
        except:
            raise exceptions.ConfigurationError('Missing identity.creatorcode')
   
        try: 
            name_parts.append('r'+str(self.revision))
        except:
            raise exceptions.ConfigurationError('Missing identity.revision')
        
        import re
        return [re.sub('[^\w\.]','_',s).lower() for s in name_parts]
       
   
    def load_from_config(self):
        pass
   
    def write_to_config(self):
        pass