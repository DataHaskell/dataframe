{-# LANGUAGE CPP #-}

{- |
Module      : DataFrame.TH
License     : MIT

Backwards-compatibility re-export hub for the split @DataFrame.TH.*@
modules. New code can import the specific submodule directly:

  * "DataFrame.TH.Records" — record/DataFrame-based splices (no file IO)
  * "DataFrame.TH.CSV"     — CSV-file-based splices (requires @-fwith-csv@)
  * "DataFrame.TH.Parquet" — Parquet-file-based splices (requires @-fwith-parquet@)

These live in @dataframe-th@, @dataframe-csv-th@, and
@dataframe-parquet-th@ respectively. The CSV/Parquet re-exports are
guarded with the @CSV_TH@ / @PARQUET_TH@ CPP defines that the
meta-@dataframe@ package sets based on its cabal flags.
-}
module DataFrame.TH (
    module DataFrame.TH.Records,
#ifdef WITH_CSV_TH
    module DataFrame.TH.CSV,
#endif
#ifdef WITH_PARQUET_TH
    module DataFrame.TH.Parquet,
#endif
) where

import DataFrame.TH.Records
#ifdef WITH_CSV_TH
import DataFrame.TH.CSV
#endif
#ifdef WITH_PARQUET_TH
import DataFrame.TH.Parquet
#endif
