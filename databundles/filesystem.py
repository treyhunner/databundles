'''
Created on Jun 23, 2012

@author: eric
'''

import os.path

from databundles.orm import File
from contextlib import contextmanager
import zipfile
import urllib
import databundles.util, logging

logger = databundles.util.get_logger(__name__)
logger.setLevel(logging.ERROR)

class DownloadFailedError(Exception):
    pass

class FileRef(File):
    '''Extends the File orm class with awareness of the filsystem'''
    def __init__(self, bundle):
        
        self.super_ = super(FileRef, self)
        self.super_.__init__()
        
        self.bundle = bundle
        
    @property
    def abs_path(self):
        return self.bundle.filesystem.path(self.path)
    
    @property
    def changed(self):
        return os.path.getmtime(self.abs_path) > self.modified
       
    def update(self):
        self.modified = os.path.getmtime(self.abs_path)
        self.content_hash = Filesystem.file_hash(self.abs_path)
        self.bundle.database.session.commit()

class Filesystem(object):
    
    def __init__(self, config):
        self.config = config
     

    def get_cache(self, cache_name, config=None):
        """Return a new :class:`FsCache` built on the configured cache directory
  
        :type cache_name: string
        :param cache_name: A key in the 'filesystem' section of the configuration,
            from which configuration will be retrieved
     
        :type config: :class:`RunConfig`
        :param config: If supplied, wil replace the default RunConfig()
              
        :rtype: a `FsCache` object
        :return: a nre first level cache. 
        
        If config is None, the function will constuct a new RunConfig() with a default
        constructor. 
        
        The `FsCache` will be constructed with the cache_dir values from the
        library.cache config key, and if the library.repository value exists, it will 
        be use for the upstream parameter.
    
        """
        
        from databundles.dbexceptions import ConfigurationError

        if config is None:
            config = self.config
    
        if not config.filesystem:
            raise ConfigurationError("Didn't get filsystem configuration value. "+
                                     " from config files: "+"; ".join(config.loaded))
    
        subconfig = config.filesystem.get(cache_name,False)
  
        if subconfig is False:
            raise ConfigurationError("Didn't get filsystem.{} configuration value"
                                     .format(cache_name))
               
        cache = self._get_cache(cache_name, subconfig)
        
        if subconfig.get('upstream',False):
            cache.upstream = self._get_cache(cache_name+'.upstream', subconfig.get('upstream'))
        
        return cache

    def _get_cache(self,config_name, config):
        from databundles.dbexceptions import ConfigurationError
        
        if config.get('dir',False):

            return FsCache(config.get('dir'), 
                           maxsize=config.get('size',10000))
            
        elif config.get('bucket',False):
            
            return S3Cache(bucket=config.get('bucket'), 
                    prefix=config.get('prefix', None),
                    access_key=config.get('access_key'),
                    secret=config.get('secret'))
            
        else:
            raise ConfigurationError("Can't determine type of cache for key: {}".format(config_name))
        
    @classmethod
    def rm_rf(cls, d):
        
        if not os.path.exists(d):
            return
        
        for path in (os.path.join(d,f) for f in os.listdir(d)):
            if os.path.isdir(path):
                cls.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)


class BundleFilesystem(Filesystem):
    
    BUILD_DIR = 'build'

    def __init__(self, bundle, root_directory = None):
        
        super(BundleFilesystem, self).__init__(bundle.config._run_config)
        
        self.bundle = bundle
        if root_directory:
            self.root_directory = root_directory
        else:
            self.root_directory = Filesystem.find_root_dir()
 
        if not os.path.exists(self.path(BundleFilesystem.BUILD_DIR)):
            os.makedirs(self.path(BundleFilesystem.BUILD_DIR),0755)
 
    @staticmethod
    def find_root_dir(testFile='bundle.yaml', start_dir =  None):
        '''Find the parent directory that contains the bundle.yaml file '''
        import sys

        if start_dir is not None:
            d = start_dir
        else:
            d = sys.path[0]
        
        while os.path.isdir(d) and d != '/':
        
            test =  os.path.normpath(d+'/'+testFile)

            if(os.path.isfile(test)):
                return d
            d = os.path.dirname(d)
             
        return None
    
    @property
    def root_dir(self):
        '''Returns the root directory of the bundle '''
        return self.root_directory

    def ref(self,rel_path):
        
        s = self.bundle.database.session
        import sqlalchemy.orm.exc
        
        try:
            o = s.query(FileRef).filter(FileRef.path==rel_path).one()
            o.bundle = self.bundle
        
            return o
        except  sqlalchemy.orm.exc.NoResultFound as e:
            raise e

    def path(self, *args):
        '''Resolve a path that is relative to the bundle root into an 
        absoulte path'''
     
        args = (self.root_directory,) +args
        try:
            p = os.path.normpath(os.path.join(*args))    
        except AttributeError as e:
            raise ValueError("Path arguments aren't valid when generating path:"+ e.message)
        dir_ = os.path.dirname(p)
        if not os.path.exists(dir_):
            try:
                os.makedirs(dir_) # MUltiple process may try to make, so it could already exist
            except Exception as e: #@UnusedVariable
                pass
            
            if not os.path.exists(dir_):
                raise Exception("Couldn't create directory "+dir_)

        return p

    def build_path(self, *args):
    
        if len(args) > 0 and args[0] == self.BUILD_DIR:
            raise ValueError("Adding build to existing build path "+os.path.join(*args))
        
        args = (self.BUILD_DIR,) + args
        return self.path(*args)


    def directory(self, rel_path):
        '''Resolve a path that is relative to the bundle root into 
        an absoulte path'''
        abs_path = self.path(rel_path)
        if(not os.path.isdir(abs_path) ):
            os.makedirs(abs_path)
        return abs_path
 
    @staticmethod
    def file_hash(path):
        '''Compute hash of a file in chunks'''
        import hashlib
        md5 = hashlib.md5()
        with open(path,'rb') as f: 
            for chunk in iter(lambda: f.read(8192), b''): 
                md5.update(chunk)
        return md5.hexdigest()
 


    def _get_unzip_file(self, cache, tmpdir, zf, path, name):
 
        name = name.replace('/','').replace('..','')
        
        base = os.path.basename(path)
        
        rel_path = (urllib.quote_plus(base.replace('/','_'),'_')+'/'+
                    urllib.quote_plus(name.replace('/','_'),'_') )
     
        # Check if it is already in the cache
        cached_file = cache.get(rel_path)
        
        if cached_file:
            return cached_file
     
        # Not in cache, extract it. 
        tmp_abs_path = os.path.join(tmpdir, name)
        
        if not os.path.exists(tmp_abs_path):
            zf.extract(name,tmpdir )
            
        # Store it in the cache.           
        abs_path = cache.put(tmp_abs_path, rel_path)
        
        # There have been zip files that have been truncated, but I don't know
        # why. this is a stab i the dark to catch it. 
        if self.file_hash(tmp_abs_path) != self.file_hash(abs_path):
            raise Exception('Zip file extract error: md5({}) != md5({})'
                            .format(tmp_abs_path,abs_path ))

        return abs_path
 
    def unzip(self,path):
        '''Context manager to extract a single file from a zip archive, and delete
        it when finished'''
        import tempfile, uuid
        
        cache = self.get_cache('extracts')

        tmpdir = tempfile.mkdtemp(str(uuid.uuid4()))
   
        try:
            with zipfile.ZipFile(path) as zf:
                name = iter(zf.namelist()).next() # Assume only one file in zip archive.    

                abs_path = self._get_unzip_file(cache, tmpdir, zf, path, name)

                return abs_path
        finally:
            self.rm_rf(tmpdir)
            
        return None

    @contextmanager
    def unzip_dir(self,path,  cache=True):
        '''Extract all of the files in a zip file to a directory, and return
        the directory. Delete the directory when done. '''
       
        raise Exception("Fixme")
       
        extractDir = self.extracts_path(os.path.basename(path))

        files = []
     
        if os.path.exists(extractDir):
            import glob
            # File already exists, so don't extract agaain. 
            yield glob.glob(extractDir+'/*')

        else :
            try:
                with zipfile.ZipFile(path) as zf:
                    for name in zf.namelist():
                  
                        extractFilename = os.path.join(extractDir, name)
                        
                        files.append(extractFilename)
                        
                        if os.path.exists(extractFilename):
                            os.remove(extractFilename)
                        
                        # don't let the name of the file escape the extract dir. 
                        name = name.replace('/','').replace('..','')
                        zf.extract(name,extractDir )
                        
                    yield files
            except Exception as e:
                if os.path.exists(path):
                    os.remove(path)
                raise e
                
        
        if  not cache and os.path.isdir(extractDir):
            import shutil
            shutil.rmtree(extractDir)
        
    def download(self,url, test_f=None):
        '''Context manager to download a file, return it for us, 
        and delete it when done.
        
        Will store the downloaded file into the cache defined
        by filesystem.download
        '''
        
        import tempfile
        import urlparse
      
        cache = self.get_cache('downloads')
        parsed = urlparse.urlparse(url)
        file_path = parsed.netloc+'/'+urllib.quote_plus(parsed.path.replace('/','_'),'_')

        # We download to a temp file, then move it into place when 
        # done. This allows the code to detect and correct partial
        # downloads. 
        download_path = os.path.join(tempfile.gettempdir(),file_path+".download")
            
        for attempts in range(3):
   
            if attempts > 0:
                self.bundle.error("Retrying download of {}".format(url))

            cached_file = None
            out_file = None
            excpt = None
                        
            try:                  
                if os.path.exists(download_path):
                    os.remove(download_path)

                cached_file = cache.get(file_path)
          
                if cached_file:
                    out_file = cached_file
                    
                    if test_f and not test_f(out_file):
                        cache.remove(file_path, True)
                        raise DownloadFailedError("Cached Download didn't pass test function "+url)
                    
                else:
                    dirname = os.path.dirname(download_path)
                    if not os.path.isdir(dirname):
                        os.makedirs(dirname)
                    
                    self.bundle.log("Downloading "+url)
                    self.bundle.log("  --> "+file_path)
                    download_path, headers = urllib.urlretrieve(url,download_path) #@UnusedVariable
                    
                    if not os.path.exists(download_path):
                        raise DownloadFailedError("Failed to download "+url)
                    
                    if test_f and not test_f(download_path):
                        raise DownloadFailedError("Download didn't pass test function "+url)
                    
                    out_file = cache.put(download_path, file_path)
       
                break
                
            except DownloadFailedError as e:
                self.bundle.error("Failed:  "+str(e))
                excpt = e
            except IOError as e:
                self.bundle.error("Failed to download "+url+" to "+file_path+" : "+str(e))
                excpt = e
            except urllib.ContentTooShortError as e:
                self.bundle.error("Content too short for "+url)
                excpt = e
            except zipfile.BadZipfile as e:
                # Code that uses the yield value -- like th filesystem.unzip method
                # can throw exceptions that will propagate to here. Unexpected, but very useful. 
                # We should probably create a FileNotValueError, but I'm lazy. 
                self.bundle.error("Got an invalid zip file for "+url)
                cache.remove(file_path)
                excpt = e
                
            except Exception as e:
                self.bundle.error("Unexpected download error '"+str(e)+"' when downloading "+url)
                raise e
                
        if download_path and os.path.exists(download_path):
            os.remove(download_path) 

        if excpt:
            raise excpt
    
        return out_file

    def get_url(self,source_url, create=False):
        '''Return a database record for a file'''
    
        import sqlalchemy.orm.exc
 
        s = self.bundle.database.session
        
        try:
            o = (s.query(File).filter(File.source_url==source_url).one())
         
        except sqlalchemy.orm.exc.NoResultFound:
            if create:
                o = File(source_url=source_url,path=source_url,process='none' )
                s.add(o)
                s.commit()
            else:
                return None
          
          
        o.session = s # Files have SavableMixin
        return o
    
    def get_or_new_url(self, source_url):
        return self.get_url(source_url, True)
 
    def add_file(self, rel_path):
        return self.filerec(rel_path, True)
    
    def filerec(self, rel_path, create=False):
        '''Return a database record for a file'''
  
        import sqlalchemy.orm.exc

        s = self.bundle.database.session
        
        if not rel_path:
            raise ValueError('Must supply rel_path')
        
        try:
            o = (s.query(File).filter(File.path==rel_path).one())
            o._is_new = False
        except sqlalchemy.orm.exc.NoResultFound as e:
           
            if not create:
                raise e
           
            a_path = self.filesystem.path(rel_path)
            o = File(path=rel_path,
                     content_hash=Filesystem.file_hash(a_path),
                     modified=os.path.getmtime(a_path),
                     process='none'
                     )
            s.add(o)
            s.commit()
            o._is_new = True
            
        except Exception as e:
            return None
        
        return o
   

class FsCache(object):
    '''A cache that transfers files to and from a remote filesystem
    
    The `FsCache` stores files in a filesystem, possily retrieving and storing
    files to an upstream cache. 
    
    When files are written , they are written through to the upstream. If a file
    is requested that does not exist, it is fetched from the upstream. 
    
    When a file is added that causes the disk usage to exceed `maxsize`, the oldest
    files are deleted to free up space. 
    
     '''


    def __init__(self, cache_dir, maxsize=10000, upstream=None):
        '''Init a new FileSystem Cache
        
        Args:
            cache_dir
            maxsize. Maximum size of the cache, in GB
        
        '''
        
        from databundles.dbexceptions import ConfigurationError

        self.cache_dir = cache_dir
        self.maxsize = int(maxsize * 1048578)  # size in MB
        self.upstream = upstream
        self._database = None
   
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        if not os.path.isdir(self.cache_dir):
            raise ConfigurationError("Cache dir '{}' is not valid".format(self.cache_dir)) 
        
    @property
    def database(self):
        import sqlite3
        
        if not self._database:
            db_path = os.path.join(self.cache_dir, 'file_database.db')
            
            if not os.path.exists(db_path):
                create_sql = """
                CREATE TABLE files(
                path TEXT UNIQUE ON CONFLICT REPLACE, 
                size INTEGER, 
                time REAL)
                """
                conn = sqlite3.connect(db_path)
                conn.execute(create_sql)
                conn.close()
                
            self._database = sqlite3.connect(db_path)
            
        return self._database
            
    @property
    def size(self):
        '''Return the size of all of the files referenced in the database'''
        c = self.database.cursor()
        r = c.execute("SELECT sum(size) FROM files")
     
        try:
            size = int(r.fetchone()[0])
        except TypeError:
            size = 0
    
        return size
        
    def _delete_to_size(self, size):
        '''Delete records, from oldest to newest, to free up space ''' 
      
        if size <= 0:
            return

        removes = []

        for row in self.database.execute("SELECT path, size, time FROM files ORDER BY time ASC"):

            if size > 0:
                removes.append(row[0])
                size -= row[1]
            else:
                break
  
        for row in removes:
            self.remove(row)

    def _free_up_space(self, size):
        '''If there are not size bytes of space left, delete files
        until there is ''' 
        
        space = self.size + size - self.maxsize # Amount of space we are over ( bytes ) for next put
        
        self._delete_to_size(space)

    def add_record(self, rel_path, size):
        import time
        c = self.database.cursor()
        c.execute("insert into files(path, size, time) values (?, ?, ?)", 
                    (rel_path, size, time.time()))
        self.database.commit()

    def verify(self):
        '''Check that the database accurately describes the state of the repository'''
        
        c = self.database.cursor()
        non_exist = set()
        
        no_db_entry = set(os.listdir(self.cache_dir))
        try:
            no_db_entry.remove('file_database.db')
            no_db_entry.remove('file_database.db-journal')
        except: 
            pass
        
        for row in c.execute("SELECT path FROM files"):
            path = row[0]
            
            repo_path = os.path.join(self.cache_dir, path)
        
            if os.path.exists(repo_path):
                no_db_entry.remove(path)
            else:
                non_exist.add(path)
            
        if len(non_exist) > 0:
            raise Exception("Found {} records in db for files that don't exist: {}"
                            .format(len(non_exist), ','.join(non_exist)))
            
        if len(no_db_entry) > 0:
            raise Exception("Found {} files that don't have db entries: {}"
                            .format(len(no_db_entry), ','.join(no_db_entry)))
        
    @property
    def repo_id(self):
        '''Return the ID for this repository'''
        import hashlib
        m = hashlib.md5()
        m.update(self.cache_dir)

        return m.hexdigest()
    
    def get_stream(self, rel_path):
        p = self.get(rel_path)
        
        if not p:
            return None
        
        return open(p)
        
    
    def get(self, rel_path):
        '''Return the file path referenced but rel_path, or None if
        it can't be found. If an upstream is declared, it will try to get the file
        from the upstream before declaring failure. 
        '''
        import shutil

        logger.debug("{} get {}".format(self.repo_id,rel_path)) 
               
        path = os.path.join(self.cache_dir, rel_path)

      
        # If is already exists in the repo, just return it. 
        if  os.path.exists(path):
            
            if not os.path.isfile(path):
                raise ValueError("Path does not point to a file")
            
            logger.debug("{} get {} found ".format(self.repo_id, path))
            return path
            
        if not self.upstream:
            # If we don't have an upstream, then we are done. 
            return None
     
        stream = self.upstream.get_stream(rel_path)
        
        if not stream:
            logger.debug("{} get not found in upstream ()".format(self.repo_id,rel_path)) 
            return None
        
        # Got a stream from upstream, so put the file in this cache. 
        dirname = os.path.dirname(path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        
        with open(path,'w') as f:
            shutil.copyfileobj(stream, f)
        
        # Since we've added a file, must keep track of the sizes. 
        size = os.path.getsize(path)
        self._free_up_space(size)
        self.add_record(rel_path, size)
        
        stream.close()
        
        if not os.path.exists(path):
            raise Exception("Failed to copy upstream data to {} ".format(path))
        
        logger.debug("{} get return from upstream {}".format(self.repo_id,rel_path)) 
        return path
    
    def put(self, source, rel_path):
        '''Copy a file to the repository
        
        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository
        
        '''
        
        import shutil
    
        repo_path = os.path.join(self.cache_dir, rel_path)
      
        if not os.path.isdir(os.path.dirname(repo_path)):
            os.makedirs(os.path.dirname(repo_path))
    
        try:
            # Try it as a file-like object
            shutil.copyfileobj(source, repo_path)
            source.seek(0)
        except AttributeError: 
            # Nope, try a filename. 
            shutil.copyfile(source, repo_path)
            
        size = os.path.getsize(repo_path)
        
        self.add_record(rel_path, size)
        
        if self.upstream:
            self.upstream.put(source, rel_path)
    
            # Only delete if there is an upstream
            self._free_up_space(size)

        return repo_path
    
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        if self.upstream:
            return self.upstream.find(query)
        else:
            return False
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        repo_path = os.path.join(self.cache_dir, rel_path)
        
        c = self.database.cursor()
        c.execute("DELETE FROM  files WHERE path = ?", (rel_path,) )
        
        if os.path.exists(repo_path):
            os.remove(repo_path)

        self.database.commit()
            
        if self.upstream and propagate :
            self.upstream.remove(rel_path)    
            
        
    def list(self, path=None):
        '''get a list of all of the files in the repository'''
        
        path = path.strip('/')
        
        raise NotImplementedError() 


class S3Cache(object):
    '''A cache that transfers files to and from an S3 bucket
    
     '''

    def __init__(self, bucket=None, access_key=None, secret=None, prefix=None):
        '''Init a new S3Cache Cache

        '''
        from boto.s3.connection import S3Connection

        self.access_key = access_key
        self.bucket_name = bucket
        self.prefix = prefix
        self.conn = S3Connection(self.access_key, secret)
        self.bucket = self.conn.get_bucket(self.bucket_name)
  
    @property
    def size(self):
        '''Return the size of all of the files referenced in the database'''
        raise NotImplementedError() 
     
    def add_record(self, rel_path, size):
        raise NotImplementedError() 

    def verify(self):
        raise NotImplementedError() 
        
    @property
    def repo_id(self):
        '''Return the ID for this repository'''
        import hashlib
        m = hashlib.md5()
        m.update(self.bucket_name)

        return m.hexdigest()
    
    def get_stream(self, rel_path):
        """Return the object as a stream"""
        from boto.s3.key import Key
        from boto.exception import S3ResponseError 
        
        import StringIO
        
        if self.prefix is not None:
            rel_path = self.prefix+"/"+rel_path
        
        k = Key(self.bucket)

        k.key = rel_path
 
        b = StringIO.StringIO()
        try:
            k.get_contents_to_file(b)
            b.seek(0)
            return b;
        except S3ResponseError as e:
            if e.status == 404:
                return None
            else:
                raise e
    
    def get(self, rel_path):
        '''Return the file path referenced but rel_path, or None if
        it can't be found. If an upstream is declared, it will try to get the file
        from the upstream before declaring failure. 
        '''
        raise NotImplementedError('Should only use the stream interface. ')
    
    def put(self, source, rel_path):
        '''Copy a file to the repository
        
        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository
        
        '''
        from boto.s3.key import Key
        
        if self.prefix is not None:
            rel_path = self.prefix+"/"+rel_path
        
        k = Key(self.bucket)

        k.key = rel_path
        try:
            k.set_contents_from_file(source)
        except AttributeError:
            k.set_contents_from_filename(source)
            
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
       
        raise NotImplementedError()
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        from boto.s3.key import Key
        
        k = Key(self.bucket)

        k.key = rel_path
        k.delete()    
        
    def list(self, path=None):
        '''get a list of all of the files in the repository'''
        
        path = path.strip('/')
        
        raise NotImplementedError() 

       
