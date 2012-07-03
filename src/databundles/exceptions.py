'''
Created on Jun 19, 2012

@author: eric
'''

class BundleError(Exception):
    
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class ConfigurationError(BundleError):
    '''Error in the configuration files'''
    
class ResultCountError(BundleError):
    '''Got too many or too few results'''
    
class FilesystemError(BundleError):
    '''Missing file, etc. '''
