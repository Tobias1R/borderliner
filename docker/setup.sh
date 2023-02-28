#!/bin/bash

# Clone the repository
git clone https://github.com/Tobias1R/borderliner.git

# Change into the cloned directory
cd borderliner

# Install any dependencies required by the setup.py script
apt-get update && apt-get install -y python3-setuptools

# Run the setup.py script to install the package
python3 setup.py install