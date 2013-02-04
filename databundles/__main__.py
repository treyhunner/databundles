"""Main script for the databaundles package, providing support for creating
new bundles

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


import os.path
import yaml
import shutil
from databundles.run import  get_runconfig

def bundle_command(args, rc):
  
    from databundles.identity import Identity
    from databundles.identity import DatasetNumber
    
    if args.subcommand == 'new':
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

def library_command(args, rc):
    import library

    l = library.get_library(name=args.name)


    if args.subcommand == 'init':
        print "Initialize Library"
        l.database.create()

    elif args.subcommand == 'server':
        from databundles.server.main import  local_run

        local_run(rc, name = args.name, reloader=False)
      
    elif args.subcommand == 'drop':
        print "Drop tables"
        l.database.drop()

    elif args.subcommand == 'clean':
        print "Clean tables"
        l.database.clean()
        
    elif args.subcommand == 'rebuild':
        print "Rebuild library"
        l.rebuild()
        
    elif args.subcommand == 'info':
        print "Library Info"
        print "Database: {}".format(l.database.dsn)
        print "Remote: {}".format(l.remote)
        print "Cache: {}".format(l.cache.cache_dir)
        
    elif args.subcommand == 'push':
        files_ = l.database.get_file_by_state('new')
        if len(files_):
            print "-- Pushing to {}".format(l.remote)
            for f in files_:
                print "Pushing: {}".format(f.path)
                l.push(f)

    elif args.subcommand == 'files':
        files_ = l.database.get_file_by_state(args.file_state)
        if len(files_):
            print "-- Display {} files".format(args.file_state)
            for f in files_:
                print f.path

    else:
        print "Unknown subcommand"
        print args 

def test_command(args,rc):
    
    if args.subcommand == 'config':
        print rc.dump()
    elif args.subcommand == 'foobar':
        pass
    else:
        print 'Testing'
        print args

def main():
    import argparse
    
    parser = argparse.ArgumentParser(prog='python -mdatabundles',
                                     description='Create new bundle soruce packages')
    
    #parser.add_argument('command', nargs=1, help='Create a new bundle') 
 
    parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file") 
    parser.add_argument('-v','--verbose', default=None, action='append', help="Be verbose") 
    parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")

  
    cmd = parser.add_subparsers(title='commands', help='command help')
    
    #
    # Bundle Command
    #
    bundle_p = cmd.add_parser('bundle', help='Create a new bundle')
    bundle_p.set_defaults(command='bundle')   
    asp = bundle_p.add_subparsers(title='Bundle commands', help='Commands for maniplulating bundles')
    
    sp = asp.add_parser('new', help='Create a new bundle')
    sp.set_defaults(subcommand='new')
    sp.set_defaults(revision='1') # Needed in Identity.name_parts
    sp.add_argument('-s','--source', required=True, help='Source, usually a domain name') 
    sp.add_argument('-d','--dataset',  required=True, help='Name of the dataset') 
    sp.add_argument('-b','--subset', nargs='?', default=None, help='Name of the subset') 
    sp.add_argument('-v','--variation', default='orig', help='Name of the variation') 
    sp.add_argument('-c','--creator',  required=True, help='Id of the creator') 
    sp.add_argument('-n','--dry-run', default=False, help='Dry run') 
    sp.add_argument('args', nargs=argparse.REMAINDER) # Get everything else. 

    #
    # Library Command
    #
    lib_p = cmd.add_parser('library', help='Manage a library')
    lib_p.set_defaults(command='library')
    asp = lib_p.add_subparsers(title='library commands', help='command help')
    lib_p.add_argument('-n','--name',  default='default',  help='Select a different name for the library')
        
    sp = asp.add_parser('push', help='Push new library files')
    sp.set_defaults(subcommand='push')
    sp.add_argument('-w','--watch',  default=False,action="store_true",  help='Check periodically for new files.')

    sp = asp.add_parser('server', help='Run the library server')
    sp.set_defaults(subcommand='server') 
    sp.add_argument('-d','--daemonize', default=False, action="store_true",   help="Run as a daemon") 
    sp.add_argument('-g','--group', default=None,   help="Set group for daemon operation") 
    sp.add_argument('-u','--user', default=None,  help="Set user for daemon operation")   
      
    sp = asp.add_parser('files', help='Print out files in the library')
    sp.set_defaults(subcommand='files')
    sp.add_argument('-n','--new',  default=False,action="store_const", const='new',  dest='file_state', help='Print new files')
    sp.add_argument('-p','--pushed',  default=False,action="store_const", const='pushed', dest='file_state',  help='Print pushed files')
    sp.add_argument('-u','--pulled',  default=False,action="store_const", const='pulled', dest='file_state',  help='Print pulled files')
 
    sp = asp.add_parser('new', help='Create a new library')
    sp.set_defaults(subcommand='new')
    
    sp = asp.add_parser('drop', help='Print out files in the library')
    sp.set_defaults(subcommand='drop')    
    
    sp = asp.add_parser('clean', help='Remove all entries from the library')
    sp.set_defaults(subcommand='clean')
    
    sp = asp.add_parser('rebuild', help='Rebuild the library database from the files in the library')
    sp.set_defaults(subcommand='rebuild')
 
    sp = asp.add_parser('info', help='Display information about the library')
    sp.set_defaults(subcommand='info')   

    #
    # Test Command
    #
    lib_p = cmd.add_parser('test', help='Test and debugging')
    lib_p.set_defaults(command='test')
    asp = lib_p.add_subparsers(title='Test commands', help='command help')
    
    sp = asp.add_parser('config', help='Dump the configuration')
    sp.set_defaults(subcommand='config')
  
                      
    args = parser.parse_args()
   
    
    if args.single_config:
        if args.config is None or len(args.config) > 1:
            raise Exception("--single_config can only be specified with one -c")
        else:
            rc_path = args.config
    elif args.config is not None and len(args.config) == 1:
            rc_path = args.config.pop()
    else:
        rc_path = args.config
        
    rc = get_runconfig(rc_path)
   
    funcs = {
        'bundle': bundle_command,
        'library':library_command,
        'test':test_command
    }
        
    f = funcs.get(args.command, False)
        
    if not f:
        print "Error: No command: "+args.command
    else:
        f(args, rc)
    
def daemonize(f, args,  rc):
        '''Run a process as a daemon'''
        import daemon
        import lockfile
        import os
        import grp, pwd
        
        lib_dir = '/var/lib/databundles'
        run_dir = '/var/lib/databundles'
        log_file = '/var/log/library-server'
        
                
        for dir in [run_dir, lib_dir]:
            if not os.path.exists(dir):
                os.makedirs(dir)

        out = open(log_file, "a")
        ubuf_out =  os.fdopen(out.fileno(), 'w', 0)

        gid =  grp.getgrnam(args.group).gr_gid if args.group is not None else os.getgid()
        uid =  pwd.getpwnam(args.group).pw_gid if args.user  is not None else os.getuid()  

        context = daemon.DaemonContext(
            working_directory=lib_dir,
            umask=0o002,
            pidfile=lockfile.FileLock(os.path.join(run_dir,'library-server.pid')),
            stdout = ubuf_out, 
            stderr = ubuf_out,
            gid  = gid, 
            uid = uid
            )

                
        # OPen the log file, then fdopen it with a zero buffer sized, to 
        # ensure the ourput is unbuffered. 
        context.open()
        
        ubuf_out.write("Starting library server as a daemon\n")
        ubuf_out.flush()
        #with context:
        f(args, rc)


if __name__ == '__main__':
    main()