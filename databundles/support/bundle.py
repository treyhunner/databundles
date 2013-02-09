'''

'''


from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    ### Meta is run before prepare, to load or configure meta information

    def meta(self):
        return True
 

    ### Prepare is run before building, part of the devel process.  

    def prepare(self):
        return True
 
    
    ### Build the final package

    def pre_build(self):
        return True
        
    def build(self):
        return True
    
    def post_build(self):
        return True
    
        
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    