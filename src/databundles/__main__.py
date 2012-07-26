'''
Main script for the databaundles package, providing support for creating
new bundles

'''


import os.path
import yaml
import shutil


def new_command(args):
  
    from databundles.identity import Identity
    from databundles.identity import DatasetNumber
    
    # Remove the creator code and version. 
    name = '-'.join(Identity.name_parts(args)[:-2])

    if not os.path.exists(name):
        os.makedirs(name)
    elif not os.path.isdir(name):
        raise IOError("Directory already exists: "+name)

    config ={'identity':{
         'id': str(DatasetNumber()),
         'source': args.source,
         'creator': args.creator,
         'dataset':args.dataset,
         'subset': args.subset,
         'variation': args.variation,
         'revision': args.revision
         }}
    
    file_ = os.path.join(name, 'bundle.yaml')
    yaml.dump(config, file(file_, 'w'), indent=4, default_flow_style=False)

    bundle_file =  os.path.join(os.path.dirname(__file__),'support','bundle.py')

    shutil.copy(bundle_file ,name  )

def library_command(args):
    if args.init:
        from library import LocalLibrary
        l = LocalLibrary()
        l.database.create()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(prog='python -mdatabundles',
                                     description='Create new bundle soruce packages')
    
    #parser.add_argument('command', nargs=1, help='Create a new bundle') 
    
    subp = parser.add_subparsers(title='commands', help='sub-command help')
    
    new_p = subp.add_parser('new', help='Create a new bundle')
    new_p.set_defaults(command='new')
    new_p.set_defaults(revision='1') # Needed in Identity.name_parts
    new_p.add_argument('-s','--source', required=True, help='Source, usually a domain name') 
    new_p.add_argument('-d','--dataset',  required=True, help='Name of the dataset') 
    new_p.add_argument('-b','--subset', nargs='?', default=None, help='Name of the subset') 
    new_p.add_argument('-v','--variation', default='orig', help='Name of the variation') 
    new_p.add_argument('-c','--creator',  required=True, help='Id of the creator') 
    new_p.add_argument('-n','--dry-run', default=False, help='Dry run') 
    new_p.add_argument('args', nargs=argparse.REMAINDER) # Get everything else. 

    lib_p = subp.add_parser('library', help='Manage a library')
    lib_p.set_defaults(command='library')
    lib_p.add_argument('-i','--init',  default=True,action="store_true",  help='Iniitalize the library specified in the configuration') 
   

    args = parser.parse_args()
   
    funcs = {
        'new':new_command,
        'library':library_command
    }
        
    if not funcs.get(args.command, False):
        print "Error: No command: "+args.command
    else:
        funcs[args.command](args)
    
if __name__ == '__main__':
    main()