#/bin/sh

rm -rf build/ Databundles.egg-info/;
python setup.py clean build sdist; 
cp dist/databundles-0.09.tar.gz  /net/nas2/c/proj/python

ssh root@lorne 'pip install --upgrade /net/nas2/c/proj/python/databundles-0.09.tar.gz'

rsync -av --exclude='build*/'  --exclude='.git' /Users/eric/proj/github.com/civicdata/ root@lorne:/build/github.com/civicdata/
