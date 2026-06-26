{-# LANGUAGE CPP #-}

{- |
Module      : DataFrame.Typed.TH
License     : MIT

Backwards-compatibility re-export hub for the split @DataFrame.Typed.TH.*@
modules:

  * "DataFrame.Typed.TH.Records" — record-based schema derivation (no file IO)
  * "DataFrame.Typed.TH.CSV"     — CSV-file-based schema derivation (requires @-fwith-csv@)

These live in @dataframe-th@ and @dataframe-csv-th@ respectively. The CSV
re-export is guarded with the @CSV_TH@ CPP define that the
meta-@dataframe@ package sets based on its cabal flags.
-}
module DataFrame.Typed.TH (
    module DataFrame.Typed.TH.Records,
#ifdef WITH_CSV_TH
    module DataFrame.Typed.TH.CSV,
#endif
#ifdef WITH_PARQUET_TH
    module DataFrame.Typed.TH.Parquet,
#endif
) where

import DataFrame.Typed.TH.Records
#ifdef WITH_CSV_TH
import DataFrame.Typed.TH.CSV
#endif
#ifdef WITH_PARQUET_TH
import DataFrame.Typed.TH.Parquet
#endif
