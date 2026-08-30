{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | The HARD GATE for the lazy engine: for a bounded source the lazy result
must be BYTE-IDENTICAL to the eager result of the same ops (bounded joins and
aggregates route through the eager fast paths). Covers a LEFT-join pipeline
(join -> groupBy -> aggregate -> sort) and a pure groupBy pipeline.
-}
module LazyParity (tests) where

import qualified Data.Map.Strict as M
import Data.Text (Text)
import qualified Data.Text as T
import qualified DataFrame as D
import DataFrame.Expression.Operators (as, (|>))
import qualified DataFrame.Functions as F
import qualified DataFrame.IO.CSV as Csv
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.Expression as E
import DataFrame.Lazy (SortOrder (Descending))
import qualified DataFrame.Lazy as L
import qualified DataFrame.Operations.Aggregation as Agg
import DataFrame.Operations.Join (JoinType (LEFT))
import qualified DataFrame.Operations.Join as Join
import qualified DataFrame.Operations.Permutation as Perm
import DataFrame.Schema (Schema (..), schemaType)
import System.Directory (removeFile)
import System.IO.Temp (emptySystemTempFile)
import Test.HUnit

ordersSchema :: Schema
ordersSchema =
    Schema $
        M.fromList
            [ ("order_id", schemaType @Int)
            , ("customer_id", schemaType @Int)
            , ("amount", schemaType @Double)
            , ("discount", schemaType @Double)
            ]

customersSchema :: Schema
customersSchema =
    Schema $
        M.fromList
            [ ("customer_id", schemaType @Int)
            , ("region", schemaType @Text)
            , ("plan", schemaType @Text)
            ]

{- | @n@ orders; ~10% reference a customer id with no matching customer row so
the LEFT join produces a Nothing group (exercises unmatched-row semantics).
-}
ordersFrame :: Int -> D.DataFrame
ordersFrame n =
    D.fromNamedColumns
        [ ("order_id", DI.fromList [0 .. n - 1])
        ,
            ( "customer_id"
            , DI.fromList
                [if i `mod` 10 == 0 then 9_000_000 + i else i `mod` 257 | i <- [0 .. n - 1]]
            )
        , ("amount", DI.fromList [fromIntegral i * 1.5 :: Double | i <- [0 .. n - 1]])
        ,
            ( "discount"
            , DI.fromList [fromIntegral (i `mod` 7) * 0.25 :: Double | i <- [0 .. n - 1]]
            )
        ]

customersFrame :: Int -> D.DataFrame
customersFrame m =
    D.fromNamedColumns
        [ ("customer_id", DI.fromList [0 .. m - 1])
        , ("region", DI.fromList [T.pack ("r" ++ show (i `mod` 4)) | i <- [0 .. m - 1]])
        , ("plan", DI.fromList [T.pack ("p" ++ show (i `mod` 3)) | i <- [0 .. m - 1]])
        ]

-- | Write a frame to a temp CSV and run an action with the path.
withCsv :: D.DataFrame -> (FilePath -> IO a) -> IO a
withCsv df k = do
    csvPath <- emptySystemTempFile "lazy_parity_.csv"
    Csv.writeCsv csvPath df
    r <- k csvPath
    removeFile csvPath
    return r

{- | LEFT join 20k orders to 257 customers -> groupBy region,plan
-> sum(amount-discount), count -> sort revenue desc.
-}
joinPipelineParity :: Test
joinPipelineParity =
    TestCase $
        withCsv (ordersFrame 20_000) $ \ordersPath ->
            withCsv (customersFrame 257) $ \customersPath -> do
                let amount = F.col @Double "amount"
                    discount = F.col @Double "discount"
                    aggs =
                        [ F.sum (amount - discount) `as` "revenue"
                        , F.count amount `as` "orders"
                        ]
                ordersDf <- Csv.readCsvWithSchema ordersSchema ordersPath
                customersDf <- Csv.readCsvWithSchema customersSchema customersPath
                let eager =
                        Perm.sortBy [Perm.Desc (E.Col @Double "revenue")] $
                            Agg.aggregate aggs $
                                Agg.groupBy ["region", "plan"] $
                                    Join.join LEFT ["customer_id"] ordersDf customersDf
                let customersQ = L.scanCsv customersSchema (T.pack customersPath)
                lazy <-
                    L.scanCsv ordersSchema (T.pack ordersPath)
                        |> (\o -> L.join LEFT "customer_id" "customer_id" o customersQ)
                        |> L.groupBy ["region", "plan"] aggs
                        |> L.sortBy [("revenue", Descending)]
                        |> L.runDataFrame
                assertEqual
                    "join pipeline lazy == eager (byte-identical)"
                    (show eager)
                    (show lazy)

-- | Pure groupBy customer_id -> sum(amount), count, sort desc.
groupByPipelineParity :: Test
groupByPipelineParity =
    TestCase $
        withCsv (ordersFrame 20_000) $ \ordersPath -> do
            let amount = F.col @Double "amount"
                aggs =
                    [ F.sum amount `as` "total"
                    , F.count amount `as` "n"
                    ]
            ordersDf <- Csv.readCsvWithSchema ordersSchema ordersPath
            let eager =
                    Perm.sortBy [Perm.Desc (E.Col @Double "total")] $
                        Agg.aggregate aggs $
                            Agg.groupBy ["customer_id"] ordersDf
            lazy <-
                L.scanCsv ordersSchema (T.pack ordersPath)
                    |> L.groupBy ["customer_id"] aggs
                    |> L.sortBy [("total", Descending)]
                    |> L.runDataFrame
            assertEqual
                "groupBy pipeline lazy == eager (byte-identical)"
                (show eager)
                (show lazy)

tests :: [Test]
tests = [joinPipelineParity, groupByPipelineParity]
