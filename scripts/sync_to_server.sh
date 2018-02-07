#!/usr/bin/env bash

target="$1"
## this script will sync the project to the remote server
rsync -rtuv $PWD/../ $target:~/projects/nocc --exclude .git/ --exclude ./pre-data/
