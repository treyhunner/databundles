#!/bin/bash

usage() {
    echo "Usage: $0 [install-directory]"
}

DATA_DIR=/data # Directory to store downloads and library.
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

# Determine whether a suitable package manager is available
echo "--- Installing base packages. May need to ask for root password"
command -v apt-get >/dev/null 2>&1; has_aptget=$?
command -v brew >/dev/null 2>&1; has_brew=$?

# Install required dependencies using appropriate package manager
if [ $has_aptget -eq 0 ]; then
    echo "--- Installing base packages with apt-get"
    sudo apt-get install -y python-setuptools sqlite3 spatialite-bin curl git
    sudo apt-get install -y gdal-bin python-gdal python-numpy python-scipy
    sudo apt-get install -y libpq-dev python-h5py libhdf5-dev hdf5-tools h5utils
elif [ `uname` = 'Darwin' ]; then
    if [ $has_brew -eq 0 ]; then
        echo "--- Installing with Homebrew"
        # Install homebrew with:
        #ruby -e "$(curl -fsSL https://raw.github.com/mxcl/homebrew/go)"
        brew install git
        brew install gdal
        brew install hdf5
        brew install spatialite-tools

    else
        echo "ERROR: For Macs, but could not find a package manager. "
        exit 1
    fi

else
    echo "ERROR: Could not determine how to install base packages"
    exit 1
fi

# Create install directory if it doesn't exist yet
if [ ! -d $install_dir ]; then
  mkdir -p $install_dir
fi

# Install pip and virtualenv Python packages system-wide
sudo easy_install pip virtualenv

# Create virtualenv
echo "--- Building virtualenv"
virtualenv --system-site-packages .

# Source the activate script to get inside the virtualenv
. $install_dir/bin/activate

# Download the data bundles with pip so the code gets installed.
echo "--- Install the databundles package from github"
pip install -e 'git+https://github.com/clarinova/databundles#egg=databundles'

# Install the basic required packages
pip install -r $install_dir/src/databundles/requirements.txt
if [ $? -ne 0 ]; then
    echo "ERROR: requirements.txt installation failed!"
    exit 1
fi

# These packages don't install propertly in the virtualenv in Ubuntu, so we
# install them at the start via apt-get, but they install OK on Mac OS X.
if [ `uname` = 'Darwin' ]; then
    pip install gdal
    pip install numpy
    pip install h5py
fi

# The actual bundles don't need to be installed
git clone https://github.com/clarinova/civicdata $install_dir/src/civicdata

# Install the /etc/databundles.yaml file
dbmanage install config -p -f --root $DATA_DIR > databundles.yaml
sudo mv databundles.yaml /etc/databundles.yaml
