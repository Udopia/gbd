#!/bin/bash

for f in $(find /home/markus/git/gbd/main/gbd_tool -name '*.py'); do 
    sudo cp -v $f /usr/local/lib/python3.5/dist-packages/gbd_tool/${f:35}
done
