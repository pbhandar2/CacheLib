#! /usr/bin/env bash

# checkout to correct version of fmt and rebuild cachelib 
cd ../../
cd cachelib/external/fmt
sudo git checkout e67f92c55cb97558f2311a5fd544b9325b4651d0
cd ../../../
sudo ./contrib/build.sh -j -d -S