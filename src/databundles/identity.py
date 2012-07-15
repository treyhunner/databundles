'''
Created on Jun 23, 2012

@author: eric
'''

import exceptions
import os.path

class Identity(object):

    def __init__(self, *args, **kwargs):
        self.from_dict(kwargs)
        
        self.name # Will trigger errors if anything is wrong
 
    def from_dict(self,d):
        self.id_ = d.get('id', d.get('id_'))
        self.source = d.get('source')
        self.dataset =  d.get('dataset')
        self.subset =  d.get('subset',None)
        self.variation =  d.get('variation','orig')
        self.creator =  d.get('creator')
        self.revision =  int(d.get('revision',1))

    def to_dict(self):
        '''Returns the identity as a dict. values that are empty are removed'''
        d =  {
             'id':self.id_,
             'source':self.source,
             'dataset':self.dataset,
             'subset':self.subset,
             'variation':self.variation,
             'creator':self.creator,
             'revision':self.revision,
             'name' : self.name
             }

        return { k:v for k,v in d.items() if v}
 
    @property
    def creatorcode(self):
        return self._creatorcode(self)
    
    @staticmethod
    def _creatorcode(o):
        import hashlib
        # Create the creator code if it was not specified. 
        
        if o.creator is None:
            raise ValueError('Got identity object with None for creator')
        
        return hashlib.sha1(o.creator).hexdigest()[0:4]
           
    @property
    def name(self):
        return self.name_str(self)
    
    @property
    def path(self):
        '''The name is a form suitable for use in a filesystem'''
        return self.path_str(self)
    
    @classmethod
    def path_str(cls,o=None):
        '''Return the path name for this bundle'''

        parts = cls.name_parts(o)
        source = parts.pop(0)
        
        return os.path.join(source, '-'.join(parts) )
    
    @classmethod
    def name_str(cls,o=None):
        
        return '-'.join(cls.name_parts(o))
    
    @staticmethod
    def name_parts(o=None):
        """Return the parts of the name as a list, for additional processing. """
        name_parts = [];
     
        if o is None:
            raise exceptions.ConfigurationError('name_parts must be given an object')  


        try: 
            if o.source is None:
                raise exceptions.ConfigurationError('Source is None ')  
            name_parts.append(o.source)
        except Exception as e:
            raise exceptions.ConfigurationError('Missing identity.source: '+str(e))  
  
        try: 
            if o.dataset is None:
                raise exceptions.ConfigurationError('Dataset is None ')  
            name_parts.append(o.dataset)
        except Exception as e:
            raise exceptions.ConfigurationError('Missing identity.dataset: '+str(e))  
        
        try: 
            if o.subset is not None:
                name_parts.append(o.subset)
        except Exception as e:
            pass
        
        try: 
            if o.variation is not None:
                name_parts.append(o.variation)
        except Exception as e:
            pass
        
        try: 
            name_parts.append(o.creatorcode)
        except AttributeError:
            # input object doesn't have 'creatorcode'
            name_parts.append(Identity._creatorcode(o))
        except Exception as e:
            raise exceptions.ConfigurationError('Missing identity.creatorcode: '+str(e))
   
        try: 
            name_parts.append('r'+str(o.revision))
        except Exception as e:
            raise exceptions.ConfigurationError('Missing identity.revision: '+str(e))  

        
        import re
        return [re.sub('[^\w\.]','_',s).lower() for s in name_parts]
       
   

