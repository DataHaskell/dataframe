{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module LazyParity (tests) where

import qualified Data.Map.Strict as M
import Data.Text (Text)
import qualified Data.Text as T
import qualified DataFrame as D
import DataFrame.Expression.Operators (as, (.=), (.>.), (|>))
import qualified DataFrame.Functions as F
import qualified DataFrame.IO.CSV as Csv
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.Expression as E
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

-- | Orders frame where about 10% reference a customer with no matching row.
ordersFrame :: Int -> D.DataFrame
ordersFrame n =
    D.fromColumns
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
    D.fromColumns
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

-- | Left-join orders to customers, then group, aggregate and sort.
joinPipelineParity :: Test
joinPipelineParity =
    TestCase $
        withCsv (ordersFrame 20_000) $ \ordersPath ->
            withCsv (customersFrame 257) $ \customersPath -> do
                let amount = F.double "amount"
                    discount = F.double "discount"
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
                        |> (\o -> L.join LEFT ["customer_id"] o customersQ)
                        |> L.groupBy ["region", "plan"]
                        |> L.aggregate aggs
                        |> L.sortBy [L.Desc (E.Col @Double "revenue")]
                        |> L.run
                assertEqual
                    "join pipeline lazy == eager"
                    (show eager)
                    (show lazy)

-- | Pure groupBy customer_id -> sum(amount), count, sort desc.
groupByPipelineParity :: Test
groupByPipelineParity =
    TestCase $
        withCsv (ordersFrame 20_000) $ \ordersPath -> do
            let amount = F.double "amount"
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
                    |> L.groupBy ["customer_id"]
                    |> L.aggregate aggs
                    |> L.sortBy [L.Desc (E.Col @Double "total")]
                    |> L.run
            assertEqual
                "groupBy pipeline lazy == eager"
                (show eager)
                (show lazy)

streamingMeanParity :: Test
streamingMeanParity =
    TestCase $
        withCsv input $ \csvPath -> do
            case DI.fromList [(0, 0) :: (Double, Int)] of
                DI.UnboxedColumn{} -> pure ()
                _ -> assertFailure "streaming mean tuple must stay unboxed"
            let valueD = F.double "amount"
                valueI = F.int "order_id"
                aggs =
                    [ F.mean valueD `as` "mean_double"
                    , F.mean valueI `as` "mean_int"
                    ]
                query =
                    (L.scanCsvStreamingWith Csv.readSeparated ordersSchema (T.pack csvPath))
                        { L.batchSize = 2
                        }
                        |> L.groupBy ["customer_id"]
                        |> L.aggregate aggs
                        |> L.sortBy [L.Desc (E.Col @Int "customer_id")]
            actual <- L.run query
            let expected =
                    Perm.sortBy [Perm.Desc (E.Col @Int "customer_id")] $
                        Agg.aggregate aggs $
                            Agg.groupBy ["customer_id"] input
            assertEqual
                "streaming partial mean == eager mean"
                (show expected)
                (show actual)
  where
    input =
        D.fromColumns
            [ ("order_id", DI.fromList [1 .. 12 :: Int])
            , ("customer_id", DI.fromList [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2 :: Int])
            ,
                ( "amount"
                , DI.fromList
                    [0.5, 10.25, -3.0, 1.5, 11.25, -1.0, 2.5, 12.25, 1.0, 3.5, 13.25, 3.0 :: Double]
                )
            , ("discount", DI.fromList (replicate 12 (0 :: Double)))
            ]

-- | Streaming (unbounded) probe side through the streaming hash join.
streamingJoinParity :: Test
streamingJoinParity =
    TestCase $
        withCsv (ordersFrame 5_000) $ \ordersPath ->
            withCsv (customersFrame 257) $ \customersPath -> do
                ordersDf <- Csv.readCsvWithSchema ordersSchema ordersPath
                customersDf <- Csv.readCsvWithSchema customersSchema customersPath
                let byOrder = Perm.sortBy [Perm.Asc (E.Col @Int "order_id")]
                    orders =
                        (L.scanCsvStreaming ordersSchema (T.pack ordersPath))
                            { L.batchSize = 512
                            }
                    renamedCustomers = D.rename "customer_id" "cid" customersDf
                    check lbl eager lazy = do
                        actual <- L.run lazy
                        assertEqual lbl (show (byOrder eager)) (show (byOrder actual))
                check
                    "streaming inner join"
                    (Join.innerJoin ["customer_id"] ordersDf customersDf)
                    (L.innerJoin ["customer_id"] orders (L.fromDataFrame customersDf))
                check
                    "streaming left join"
                    (Join.leftJoin ["customer_id"] ordersDf customersDf)
                    (L.leftJoin ["customer_id"] orders (L.fromDataFrame customersDf))
                check
                    "streaming joinOn"
                    (Join.joinOn LEFT [("customer_id", "cid")] ordersDf renamedCustomers)
                    ( L.joinOn
                        LEFT
                        [("customer_id", "cid")]
                        orders
                        (L.fromDataFrame renamedCustomers)
                    )

-- | New plan nodes under a scan, where pruning and pushdown are active.
optimizedNodesParity :: Test
optimizedNodesParity =
    TestCase $
        withCsv (ordersFrame 3_000) $ \ordersPath -> do
            df <- Csv.readCsvWithSchema ordersSchema ordersPath
            let scan = (L.scanCsv ordersSchema (T.pack ordersPath)){L.batchSize = 256}
                amount = F.double "amount"
                check lbl eager lazy = do
                    actual <- L.run lazy
                    assertEqual lbl (show eager) (show actual)
            check
                "rename, filter on new name, select"
                ( D.select ["amt", "order_id"] $
                    D.filter (F.double "amt" .>. 1000) $
                        D.rename "amount" "amt" df
                )
                ( L.select ["amt", "order_id"] $
                    L.filter (F.double "amt" .>. 1000) $
                        L.rename "amount" "amt" scan
                )
            check
                "filter above drop stays above"
                (D.filter (amount .>. 100) (D.drop 50 df))
                (L.filter (amount .>. 100) (L.drop 50 scan))
            check
                "filter above apply moves below it"
                ( D.select ["order_id", "discount"] $
                    D.filter (amount .>. 4000) $
                        D.apply (* (2 :: Double)) "discount" df
                )
                ( L.select ["order_id", "discount"] $
                    L.filter (amount .>. 4000) $
                        L.apply (* (2 :: Double)) "discount" scan
                )
            check
                "filter on applied column stays above"
                ( D.filter
                    (F.double "discount" .>. 2)
                    (D.apply (* (2 :: Double)) "discount" df)
                )
                ( L.filter
                    (F.double "discount" .>. 2)
                    (L.apply (* (2 :: Double)) "discount" scan)
                )
            check
                "exclude then select"
                (D.select ["order_id"] (D.exclude ["discount"] df))
                (L.select ["order_id"] (L.exclude ["discount"] scan))
            check
                "filter above takeLast stays above"
                (D.filter (amount .>. 10) (D.takeLast 100 df))
                (L.filter (amount .>. 10) (L.takeLast 100 scan))
            check
                "deriveMany then select a derived column"
                ( D.select ["x"] $
                    D.deriveMany ["x" .= (amount * 2), "y" .= amount] df
                )
                ( L.select ["x"] $
                    L.deriveMany ["x" .= (amount * 2), "y" .= amount] scan
                )
            check
                "append two scans"
                (df <> D.take 10 df)
                (scan <> L.take 10 scan)

tests :: [Test]
tests =
    [ optimizedNodesParity
    , joinPipelineParity
    , groupByPipelineParity
    , streamingMeanParity
    , streamingJoinParity
    ]
