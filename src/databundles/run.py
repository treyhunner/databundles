'''
Created on Jun 10, 2012

@author: eric
'''
import sys, getopt

def get_args(argv):
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["long1=","long2="])
    except getopt.GetoptError:
        print 'test.py -i <inputfile> -o <outputfile>'
        sys.exit(2)
    for opt, arg in opts:
        print opt+" "+arg

       
    return opts, args 

def run(argv, bundle_class):
    
    print argv
    
    opts, args = get_args(argv)

    if len(args) == 0:
        args.append('all')
        
    phase = args.pop(0)
   
    b = bundle_class()

    if phase == 'all':
        phases = ['prepare','build']
    else:
        phases = [phase]
 
    
 
    if 'prepare' in phases:
        if b.pre_prepare():
            b.log("---- Preparing ----")
            if b.prepare():
                b.post_prepare()
                b.log("---- Done Preparing ----")
            else:
                b.log("---- Prepare exited with failure ----")
        else:
            b.log("---- Skipping prepare ---- ")
    else:
        b.log("---- Skipping prepare ---- ") 
        
    if 'build' in phases:
        if b.pre_build():
            if b.build():
                b.log("---- Build ---")
                b.post_build()
                b.log("---- Done Building ---")
            else:
                b.log("---- Build exited with failure ---")
        else:
            b.log("---- Skipping Build ---- ")
    else:
        b.log("---- Skipping Build ---- ") 
                
                
    