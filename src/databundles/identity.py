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
        return self._creatorcode(self)
    
    @staticmethod
    def _creatorcode(o):
        import hashlib
        # Create the creator code if it was not specified. 
        return hashlib.sha1(o.creator).hexdigest()[0:4]
       
    
       
    @property
    def name(self):
        return  self.name_str(self)
    
    @classmethod
    def name_str(cls,o):
        return '-'.join(cls.name_parts(o))
    
    @staticmethod
    def name_parts(o):
        """Return the parts of the name as a list, for additional processing. """
        name_parts = [];
     
        if o is None:
            o = self

     
        try: 
            name_parts.append(o.source)
        except Exception as e:
            raise exceptions.ConfigurationError('Missing identity.source: '+str(e))  
  
        try: 
            name_parts.append(o.dataset)
        except Exception as e:
            raise exceptions.ConfigurationError('Missing identity.dataset: '+str(e))  
        
        try: 
            name_parts.append(o.subset)
        except Exception as e:
            pass
        
        try: 
            name_parts.append(o.variation)
        except Exception as e:
            pass
        
        try: 
            name_parts.append(o.creatorcode)
        except Exception as e:
            raise exceptions.ConfigurationError('Missing identity.creatorcode: '+str(e))
   
        try: 
            name_parts.append('r'+str(o.revision))
        except:
            raise exceptions.ConfigurationError('Missing identity.revision: '+str(e))  
        
        import re
        return [re.sub('[^\w\.]','_',s).lower() for s in name_parts]
       
   
    def load_from_config(self):
        pass
   
    def write_to_config(self):
        pass