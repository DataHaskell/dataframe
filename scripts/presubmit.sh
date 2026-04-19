#!/bin/bash

set -e

./scripts/format.sh
./scripts/lint.sh

cabal build
cabal test

cd ./dataframe-persistent

cabal build all

cd ../dataframe-hasktorch

cabal build all

cd ../examples

cabal build all

cd ../dataframe-fastcsv

cabal build all
