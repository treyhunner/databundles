'''

'''


from  databundles.bundle import Bundle as Base
 

class Bundle(Base):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    ### Prepare is run before building, part of the devel process.  

    def pre_prepare(self):
        return True

    def prepare(self):
        return True
    
    def post_prepare(self):
        return True
   
   
    
    ### Build the final package

    def pre_build(self):
        return True
        
    def build(self):
        return True
    
    def post_build(self):
        return True
    
        
    ### Submit the package to the repository
 
    def pre_submit(self):
        return True
    
    def submit(self):
        return True
        
    def post_submit(self):
        return True
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    