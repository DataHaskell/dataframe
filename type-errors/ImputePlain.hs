{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Must NOT compile: 'impute' requires a nullable expression.
module ImputePlain where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

df :: D.DataFrame
df = D.fromNamedColumns [("plain", D.fromList [10 :: Int, 20, 30])]

badImpute :: D.DataFrame
badImpute = D.impute (F.col @Int "plain") 0 df
