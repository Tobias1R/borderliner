#!/bin/bash
cd /tmp
rm -Rf borderliner
# Clone the repository
#git clone https://github.com/Tobias1R/borderliner.git
cp -R /repo/business-intelligence-v2/borderliner /tmp/borderliner
# Change into the cloned directory
cd /tmp/borderliner

# Install any dependencies required by the setup.py script
apt-get update && apt-get install -y python3-setuptools

# Run the setup.py script to install the package
python3 /tmp/borderliner/setup.py install
