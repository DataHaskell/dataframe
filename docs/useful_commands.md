# Useful dev commands

* `cabal build dataframe-core --ghc-options="-fforce-recomp -ddump-simpl -ddump-rule-firings -ddump-to-file -dsuppress-all -dppr-cols200"` - compile with dump to insoect whether something fuses. The find results with `find dist-newstyle -path "*src-internal/DataFrame/Internal/Column.dump-simpl"`