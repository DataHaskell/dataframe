{-# LANGUAGE GADTs            #-}
{-# LANGUAGE TypeApplications #-}

module GenDataFrame where

import qualified Data.Map                     as M
import qualified Data.Text                    as T
import qualified Data.Vector                  as V
import qualified Data.Vector.Unboxed          as VU
import           DataFrame.Internal.Column
import           DataFrame.Internal.DataFrame
import           Test.QuickCheck

genColumn :: Int -> Gen Column
genColumn len =
    oneof
        [ BoxedColumn . V.fromList <$> vectorOf len (arbitrary @Int)
        , UnboxedColumn . VU.fromList <$> vectorOf len (arbitrary @Double)
        , OptionalColumn . V.fromList <$> vectorOf len (arbitrary @(Maybe Int))
        ]

genDataFrame :: Gen DataFrame
genDataFrame = do
    numRows <- choose (0, 100)
    numCols <- choose (0, 10)
    colNames <- V.fromList <$> vectorOf numCols genUniqueColName
    cols <- V.fromList <$> vectorOf numCols (genColumn numRows)
    let indices = M.fromList $ zip (V.toList colNames) [0 ..]
    pure $
        DataFrame
            { columns = cols
            , columnIndices = indices
            , dataframeDimensions = (numRows, numCols)
            , derivingExpressions = M.empty
            }

genUniqueColName :: Gen T.Text
genUniqueColName = T.pack <$> listOf1 (elements ['a' .. 'z'])

instance Arbitrary DataFrame where
    arbitrary = genDataFrame
    shrink df = []
