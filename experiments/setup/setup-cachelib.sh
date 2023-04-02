#! /usr/bin/env bash

cd ${HOME}
if [ ! -d "${HOME}/CacheLib" ]; then
    git clone https://github.com/pbhandar2/CacheLib
fi
cd CacheLib 
git checkout replay 
./contrib/build.sh -j -d 