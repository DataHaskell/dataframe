#!/bin/bash

cabal repl dataframe --repl-options=-fobject-code -O2 --build-depends=text --build-depends=vector --build-depends=bytestring --build-depends=time