#!/bin/bash 
DATA_DIR=/data # Directory to stor downloads and library. 
while getopts "d:" OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         d)
             DATA_DIR="-i $OPTARG"
             ;;
         ?)
             usage
             exit
             ;;
     esac
done
shift $((OPTIND-1))

install_dir=$1

if [ -z "$install_dir" ]; then
    echo "ERROR: Must supply a directory name"
    exit 1
fi

echo "Installing base packages. May need to ask for root password"
command -v foo >/dev/null 2>&1
if [ $? eq 0 ]; then
    sudo apt-get install -y libpq-dev gdal-bin sqlite3 spatialite-bin curl git
    sudo apt-get install -y python-gdal python-h5py python-numpy python-scipy
elif [ `uname` = 'Darwin ']; then
    echo "For Macs"
fi

if [ ! -d $install_dir ]; then
  mkdir -p $install_dir  
fi

# Basic virtualenv setup.
mkdir -p $install_dir/bin
curl https://raw.github.com/pypa/virtualenv/master/virtualenv.py > $install_dir/bin/virtualenv.py
/usr/bin/python $install_dir/bin/virtualenv.py --system-site-packages $install_dir
  
# Source the activate script to get it going
. $install_dir/bin/activate
 
# Download the data bundles with pip so the code gets installed. 
pip install -e 'git+https://github.com/clarinova/databundles#egg=databundles'

# Install the basic required packages
pip install -r $install_dir/src/databundles/requirements.txt

# The actual bundles don't need to be installed
git clone https://github.com/clarinova/civicdata $install_dir/src/civicdata
 
# Install the /etc/databundles.yaml file
dbmanage install config -p --root $DATA_DIR > databundles.yaml
sudo mv databundles.yaml /etc/databundles.yaml