{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Every operation, run through eager, typed, lazy and typed lazy APIs.
module Parity.FourWay (tests) where

import qualified Data.Text as T
import System.Random (StdGen, mkStdGen)

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Lazy as L
import qualified DataFrame.Typed as DT
import qualified DataFrame.Typed.Functions as DTF
import qualified DataFrame.Typed.Lazy as TL

import Test.HUnit

type Orders =
    '[ '("cid", Int)
     , '("region", T.Text)
     , '("amount", Double)
     ]

type Customers =
    '[ '("cid", Int)
     , '("region", T.Text)
     , '("tier", T.Text)
     ]

type Users = '[ '("uid", Int), '("name", T.Text)]

ordersDF :: D.DataFrame
ordersDF =
    D.fromColumns
        [ ("cid", DI.fromList [1, 2, 2, 3, 5, 1 :: Int])
        , ("region", DI.fromList ["eu", "us", "us", "eu", "us", "us" :: T.Text])
        , ("amount", DI.fromList [10, 20, 5, 7.5, 3, 1 :: Double])
        ]

customersDF :: D.DataFrame
customersDF =
    D.fromColumns
        [ ("cid", DI.fromList [1, 2, 3, 4 :: Int])
        , ("region", DI.fromList ["eu", "us", "eu", "eu" :: T.Text])
        , ("tier", DI.fromList ["gold", "free", "free", "gold" :: T.Text])
        ]

usersDF :: D.DataFrame
usersDF =
    D.fromColumns
        [ ("uid", DI.fromList [1, 2, 4 :: Int])
        , ("name", DI.fromList ["ann", "bo", "cy" :: T.Text])
        ]

type Nullable = '[ '("k", Int), '("v", Maybe Double), '("w", Maybe Int)]

nullableDF :: D.DataFrame
nullableDF =
    D.fromColumns
        [ ("k", DI.fromList [1, 2, 3, 4, 5 :: Int])
        , ("v", DI.fromList [Just 1.5, Nothing, Just 3, Nothing, Just 5 :: Maybe Double])
        , ("w", DI.fromList [Nothing, Nothing, Just 3, Nothing, Just 1 :: Maybe Int])
        ]

nullableT :: DT.TypedDataFrame Nullable
nullableT = either (error . show) id (DT.freezeWithError nullableDF)

tln :: TL.TypedLazyDataFrame Nullable
tln = TL.fromDataFrame nullableT

ordersT :: DT.TypedDataFrame Orders
ordersT = either (error . show) id (DT.freezeWithError ordersDF)

customersT :: DT.TypedDataFrame Customers
customersT = either (error . show) id (DT.freezeWithError customersDF)

usersT :: DT.TypedDataFrame Users
usersT = either (error . show) id (DT.freezeWithError usersDF)

-- | Compare all four results against the eager one.
fourWay ::
    String ->
    D.DataFrame ->
    DT.TypedDataFrame cols ->
    L.LazyDataFrame ->
    TL.TypedLazyDataFrame cols ->
    Test
fourWay lbl eager typed lazy typedLazy = TestCase $ do
    sameFrame (lbl ++ ": typed == eager") eager (DT.thaw typed)
    lazyDf <- L.run lazy
    sameFrame (lbl ++ ": lazy == eager") eager lazyDf
    typedLazyDf <- TL.run typedLazy
    sameFrame (lbl ++ ": typed lazy == eager") eager (DT.thaw typedLazyDf)

-- | Equal values and equal column order ('==' ignores the order).
sameFrame :: String -> D.DataFrame -> D.DataFrame -> Assertion
sameFrame lbl expected actual =
    assertEqual
        lbl
        (D.columnNames expected, expected)
        (D.columnNames actual, actual)

-- | Two-row batches, so streaming ops cross batch boundaries.
lazyOf :: D.DataFrame -> L.LazyDataFrame
lazyOf df = (L.fromDataFrame df){L.batchSize = 2}

lo, lc, lu, ln :: L.LazyDataFrame
lo = lazyOf ordersDF
lc = lazyOf customersDF
lu = lazyOf usersDF
ln = lazyOf nullableDF

tlo :: TL.TypedLazyDataFrame Orders
tlo = TL.fromDataFrame ordersT

tlc :: TL.TypedLazyDataFrame Customers
tlc = TL.fromDataFrame customersT

tlu :: TL.TypedLazyDataFrame Users
tlu = TL.fromDataFrame usersT

joins :: Test
joins =
    TestList
        [ fourWay
            "innerJoin multi-key"
            (D.innerJoin ["cid", "region"] ordersDF customersDF)
            (DT.innerJoin @'["cid", "region"] ordersT customersT)
            (L.innerJoin ["cid", "region"] lo lc)
            (TL.innerJoin @'["cid", "region"] tlo tlc)
        , fourWay
            "join INNER"
            (D.join D.INNER ["cid"] ordersDF customersDF)
            (DT.join @'DT.INNER @'["cid"] ordersT customersT)
            (L.join L.INNER ["cid"] lo lc)
            (TL.join @'TL.INNER @'["cid"] tlo tlc)
        , fourWay
            "leftJoin"
            (D.leftJoin ["cid"] ordersDF customersDF)
            (DT.leftJoin @'["cid"] ordersT customersT)
            (L.leftJoin ["cid"] lo lc)
            (TL.leftJoin @'["cid"] tlo tlc)
        , fourWay
            "rightJoin"
            (D.rightJoin ["cid"] ordersDF customersDF)
            (DT.rightJoin @'["cid"] ordersT customersT)
            (L.rightJoin ["cid"] lo lc)
            (TL.rightJoin @'["cid"] tlo tlc)
        , fourWay
            "fullOuterJoin"
            (D.fullOuterJoin ["cid"] ordersDF customersDF)
            (DT.fullOuterJoin @'["cid"] ordersT customersT)
            (L.fullOuterJoin ["cid"] lo lc)
            (TL.fullOuterJoin @'["cid"] tlo tlc)
        , fourWay
            "joinOn differing key names"
            (D.joinOn D.LEFT [("cid", "uid")] ordersDF usersDF)
            (DT.joinOn @'DT.LEFT @'[ '("cid", "uid")] ordersT usersT)
            (L.joinOn L.LEFT [("cid", "uid")] lo lu)
            (TL.joinOn @'TL.LEFT @'[ '("cid", "uid")] tlo tlu)
        ]

groupAndSort :: Test
groupAndSort =
    TestList
        [ fourWay
            "groupBy + aggregate + sortBy"
            ( ordersDF
                D.|> D.groupBy ["region"]
                D.|> D.aggregate
                    [ F.sum (F.double "amount") `D.as` "total"
                    , F.count (F.double "amount") `D.as` "n"
                    ]
                D.|> D.sortBy [D.Asc (F.text "region")]
            )
            ( ordersT
                DT.|> DT.groupBy @'["region"]
                DT.|> DT.aggregate
                    ( DT.as @"n" (DTF.count (DT.col @"amount"))
                        . DT.as @"total" (DTF.sum (DT.col @"amount"))
                    )
                DT.|> DT.sortBy [DT.Asc (DT.col @"region")]
            )
            ( lo
                L.|> L.groupBy ["region"]
                L.|> L.aggregate
                    [ F.sum (F.double "amount") `L.as` "total"
                    , F.count (F.double "amount") `L.as` "n"
                    ]
                L.|> L.sortBy [L.Asc (L.text "region")]
            )
            ( tlo
                TL.|> TL.groupBy @'["region"]
                TL.|> TL.aggregate
                    ( TL.as @"n" (DTF.count (TL.col @"amount"))
                        . TL.as @"total" (DTF.sum (TL.col @"amount"))
                    )
                TL.|> TL.sortBy [TL.Asc (TL.col @"region")]
            )
        , fourWay
            "sortBy expression, filter, derive, select, take"
            ( ordersDF
                D.|> D.filter (F.double "amount" D..>. 2)
                D.|> D.derive "twice" (F.double "amount" * 2)
                D.|> D.sortBy [D.Desc (F.double "amount" * (-1))]
                D.|> D.select ["cid", "twice"]
                D.|> D.take 3
            )
            ( ordersT
                DT.|> DT.filter (DT.col @"amount" DT..>. 2)
                DT.|> DT.derive @"twice" (DT.col @"amount" * 2)
                DT.|> DT.sortBy [DT.Desc (DT.col @"amount" * (-1))]
                DT.|> DT.select @'["cid", "twice"]
                DT.|> DT.take 3
            )
            ( lo
                L.|> L.filter (F.double "amount" L..>. 2)
                L.|> L.derive "twice" (F.double "amount" * 2)
                L.|> L.sortBy [L.Desc (F.double "amount" * (-1))]
                L.|> L.select ["cid", "twice"]
                L.|> L.take 3
            )
            ( tlo
                TL.|> TL.filter (TL.col @"amount" TL..>. 2)
                TL.|> TL.derive @"twice" (TL.col @"amount" * 2)
                TL.|> TL.sortBy [TL.Desc (TL.col @"amount" * (-1))]
                TL.|> TL.select @'["cid", "twice"]
                TL.|> TL.take 3
            )
        ]

-- | Untyped eager vs untyped lazy.
untypedPair :: String -> D.DataFrame -> L.LazyDataFrame -> Test
untypedPair lbl eager lazy = TestCase $ do
    lazyDf <- L.run lazy
    sameFrame (lbl ++ ": lazy == eager") eager lazyDf

-- | Typed eager vs typed lazy.
typedPair ::
    String -> DT.TypedDataFrame cols -> TL.TypedLazyDataFrame cols -> Test
typedPair lbl typed typedLazy = TestCase $ do
    typedLazyDf <- TL.run typedLazy
    sameFrame (lbl ++ ": typed lazy == typed") (DT.thaw typed) (DT.thaw typedLazyDf)

-- | A value computed four ways.
valueFourWay :: (Eq a, Show a) => String -> a -> a -> IO a -> IO a -> Test
valueFourWay lbl eager typed lazy typedLazy = TestCase $ do
    assertEqual (lbl ++ ": typed") eager typed
    lazyV <- lazy
    assertEqual (lbl ++ ": lazy") eager lazyV
    typedLazyV <- typedLazy
    assertEqual (lbl ++ ": typed lazy") eager typedLazyV

g :: StdGen
g = mkStdGen 42

rowOps :: Test
rowOps =
    TestList
        [ fourWay
            "filterBy"
            (D.filterBy (> 4) (F.double "amount") ordersDF)
            (DT.filterBy (> 4) (DT.col @"amount") ordersT)
            (L.filterBy (> 4) (F.double "amount") lo)
            (TL.filterBy (> 4) (TL.col @"amount") tlo)
        , fourWay
            "filterNothing"
            (D.filterNothing "v" nullableDF)
            (DT.filterNothing @"v" nullableT)
            (L.filterNothing "v" ln)
            (TL.filterNothing @"v" tln)
        , fourWay
            "filterAllNothing"
            (D.filterAllNothing (D.exclude ["k"] nullableDF))
            (DT.filterAllNothing (DT.exclude @'["k"] nullableT))
            (L.filterAllNothing (L.exclude ["k"] ln))
            (TL.filterAllNothing (TL.exclude @'["k"] tln))
        , fourWay
            "filterJust"
            (D.filterJust "v" nullableDF)
            (DT.filterJust @"v" nullableT)
            (L.filterJust "v" ln)
            (TL.filterJust @"v" tln)
        , fourWay
            "filterAllJust"
            (D.filterAllJust nullableDF)
            (DT.filterAllJust nullableT)
            (L.filterAllJust ln)
            (TL.filterAllJust tln)
        , fourWay
            "drop"
            (D.drop 3 ordersDF)
            (DT.drop 3 ordersT)
            (L.drop 3 lo)
            (TL.drop 3 tlo)
        , fourWay
            "takeLast"
            (D.takeLast 2 ordersDF)
            (DT.takeLast 2 ordersT)
            (L.takeLast 2 lo)
            (TL.takeLast 2 tlo)
        , fourWay
            "dropLast"
            (D.dropLast 2 ordersDF)
            (DT.dropLast 2 ordersT)
            (L.dropLast 2 lo)
            (TL.dropLast 2 tlo)
        , fourWay
            "range"
            (D.range (1, 4) ordersDF)
            (DT.range (1, 4) ordersT)
            (L.range (1, 4) lo)
            (TL.range (1, 4) tlo)
        , fourWay
            "range clamped"
            (D.range (-2, 100) ordersDF)
            (DT.range (-2, 100) ordersT)
            (L.range (-2, 100) lo)
            (TL.range (-2, 100) tlo)
        , fourWay
            "cube"
            (D.cube (3, 2) ordersDF)
            (DT.cube (3, 2) ordersT)
            (L.cube (3, 2) lo)
            (TL.cube (3, 2) tlo)
        , fourWay
            "topBy"
            (D.topBy 1 3 [D.Desc (F.double "amount")] ordersDF)
            (DT.topBy 1 3 [DT.Desc (DT.col @"amount")] ordersT)
            (L.topBy 1 3 [L.Desc (F.double "amount")] lo)
            (TL.topBy 1 3 [TL.Desc (TL.col @"amount")] tlo)
        , fourWay
            "distinct"
            (D.distinct (D.select ["cid", "region"] ordersDF))
            (DT.distinct (DT.select @'["cid", "region"] ordersT))
            (L.distinct (L.select ["cid", "region"] lo))
            (TL.distinct (TL.select @'["cid", "region"] tlo))
        , fourWay
            "sample"
            (D.sample g 0.5 ordersDF)
            (DT.sample g 0.5 ordersT)
            (L.sample g 0.5 lo)
            (TL.sample g 0.5 tlo)
        , fourWay
            "shuffle"
            (D.shuffle g ordersDF)
            (DT.shuffle g ordersT)
            (L.shuffle g lo)
            (TL.shuffle g tlo)
        , fourWay
            "selectRows"
            (D.selectRows [4, 0, 2] ordersDF)
            (DT.selectRows [4, 0, 2] ordersT)
            (L.selectRows [4, 0, 2] lo)
            (TL.selectRows [4, 0, 2] tlo)
        , fourWay
            "stratifiedSample"
            (D.stratifiedSample g 0.5 (F.text "region") ordersDF)
            (DT.stratifiedSample g 0.5 (DT.col @"region") ordersT)
            (L.stratifiedSample g 0.5 (F.text "region") lo)
            (TL.stratifiedSample g 0.5 (TL.col @"region") tlo)
        , fourWay
            "randomSplit fst"
            (fst (D.randomSplit g 0.5 ordersDF))
            (fst (DT.randomSplit g 0.5 ordersT))
            (fst (L.randomSplit g 0.5 lo))
            (fst (TL.randomSplit g 0.5 tlo))
        , fourWay
            "randomSplit snd"
            (snd (D.randomSplit g 0.5 ordersDF))
            (snd (DT.randomSplit g 0.5 ordersT))
            (snd (L.randomSplit g 0.5 lo))
            (snd (TL.randomSplit g 0.5 tlo))
        , fourWay
            "stratifiedSplit snd"
            (snd (D.stratifiedSplit g 0.5 (F.text "region") ordersDF))
            (snd (DT.stratifiedSplit g 0.5 (DT.col @"region") ordersT))
            (snd (L.stratifiedSplit g 0.5 (F.text "region") lo))
            (snd (TL.stratifiedSplit g 0.5 (TL.col @"region") tlo))
        , fourWay
            "kFolds last"
            (last (D.kFolds g 3 ordersDF))
            (last (DT.kFolds g 3 ordersT))
            (last (L.kFolds g 3 lo))
            (last (TL.kFolds g 3 tlo))
        ]

columnOps :: Test
columnOps =
    TestList
        [ fourWay
            "deriveMany"
            ( D.deriveMany
                [ "a2" D..= (F.double "amount" * 2)
                , "c1" D..= (F.int "cid" + 1)
                ]
                ordersDF
            )
            ( DT.deriveMany
                (DT.as @"c1" (DT.col @"cid" + 1) . DT.as @"a2" (DT.col @"amount" * 2))
                ordersT
            )
            ( L.deriveMany
                [ "a2" L..= (F.double "amount" * 2)
                , "c1" L..= (F.int "cid" + 1)
                ]
                lo
            )
            ( TL.deriveMany
                (TL.as @"c1" (TL.col @"cid" + 1) . TL.as @"a2" (TL.col @"amount" * 2))
                tlo
            )
        , fourWay
            "deriveWithExpr"
            (snd (D.deriveWithExpr "a2" (F.double "amount" * 2) ordersDF))
            (snd (DT.deriveWithExpr @"a2" (DT.col @"amount" * 2) ordersT))
            (snd (L.deriveWithExpr "a2" (F.double "amount" * 2) lo))
            (snd (TL.deriveWithExpr @"a2" (TL.col @"amount" * 2) tlo))
        , fourWay
            "exclude"
            (D.exclude ["region"] ordersDF)
            (DT.exclude @'["region"] ordersT)
            (L.exclude ["region"] lo)
            (TL.exclude @'["region"] tlo)
        , fourWay
            "rename then filter on new name"
            (D.filter (F.double "amt" D..>. 4) (D.rename "amount" "amt" ordersDF))
            (DT.filter (DT.col @"amt" DT..>. 4) (DT.rename @"amount" @"amt" ordersT))
            (L.filter (F.double "amt" L..>. 4) (L.rename "amount" "amt" lo))
            (TL.filter (TL.col @"amt" TL..>. 4) (TL.rename @"amount" @"amt" tlo))
        , fourWay
            "renameMany then select"
            (D.select ["c"] (D.renameMany [("cid", "c"), ("region", "r")] ordersDF))
            (DT.select @'["c"] (DT.renameMany @'[ '("cid", "c"), '("region", "r")] ordersT))
            (L.select ["c"] (L.renameMany [("cid", "c"), ("region", "r")] lo))
            (TL.select @'["c"] (TL.renameMany @'[ '("cid", "c"), '("region", "r")] tlo))
        , fourWay
            "cloneColumn"
            (D.cloneColumn "cid" "cid2" ordersDF)
            (DT.cloneColumn @"cid" @"cid2" ordersT)
            (L.cloneColumn "cid" "cid2" lo)
            (TL.cloneColumn @"cid" @"cid2" tlo)
        , fourWay
            "dropColumn / exclude"
            (D.exclude ["amount"] ordersDF)
            (DT.dropColumn @"amount" ordersT)
            (L.exclude ["amount"] lo)
            (TL.dropColumn @"amount" tlo)
        , fourWay
            "insert"
            (D.insert "n" [10 .. 15 :: Int] ordersDF)
            (DT.insert @"n" [10 .. 15 :: Int] ordersT)
            (L.insert "n" [10 .. 15 :: Int] lo)
            (TL.insert @"n" [10 .. 15 :: Int] tlo)
        , fourWay
            "insertWithDefault"
            (D.insertWithDefault 0 "n" [1, 2 :: Int] ordersDF)
            (DT.insertWithDefault @"n" 0 [1, 2 :: Int] ordersT)
            (L.insertWithDefault 0 "n" [1, 2 :: Int] lo)
            (TL.insertWithDefault @"n" 0 [1, 2 :: Int] tlo)
        , fourWay
            "apply"
            (D.apply (show :: Int -> String) "cid" ordersDF)
            (DT.apply @"cid" (show :: Int -> String) ordersT)
            (L.apply (show :: Int -> String) "cid" lo)
            (TL.apply @"cid" (show :: Int -> String) tlo)
        , fourWay
            "applyMany"
            (D.applyMany (* (2 :: Double)) ["amount"] ordersDF)
            (DT.applyMany @'["amount"] (* 2) ordersT)
            (L.applyMany (* (2 :: Double)) ["amount"] lo)
            (TL.applyMany @'["amount"] (* 2) tlo)
        , fourWay
            "applyWhere"
            ( D.applyWhere
                (== ("us" :: T.Text))
                "region"
                (+ (100 :: Double))
                "amount"
                ordersDF
            )
            (DT.applyWhere @"region" @"amount" (== "us") (+ 100) ordersT)
            (L.applyWhere (== ("us" :: T.Text)) "region" (+ (100 :: Double)) "amount" lo)
            (TL.applyWhere @"region" @"amount" (== "us") (+ 100) tlo)
        , fourWay
            "applyAtIndex"
            (D.applyAtIndex 3 (negate :: Double -> Double) "amount" ordersDF)
            (DT.applyAtIndex @"amount" 3 negate ordersT)
            (L.applyAtIndex 3 (negate :: Double -> Double) "amount" lo)
            (TL.applyAtIndex @"amount" 3 negate tlo)
        , fourWay
            "imputeWith"
            (D.imputeWith F.mean (F.maybeDouble "v") nullableDF)
            (DT.imputeWith DTF.mean (DT.col @"v") nullableT)
            (L.imputeWith F.mean (F.maybeDouble "v") ln)
            (TL.imputeWith DTF.mean (TL.col @"v") tln)
        , untypedPair
            "impute (untyped)"
            (D.impute (F.maybeDouble "v") 0 nullableDF)
            (L.impute (F.maybeDouble "v") 0 ln)
        , typedPair
            "impute (typed)"
            (DT.impute @"v" 0 nullableT)
            (TL.impute @"v" 0 tln)
        , typedPair
            "replaceColumn"
            (DT.replaceColumn @"amount" (DT.col @"amount" * 3) ordersT)
            (TL.replaceColumn @"amount" (TL.col @"amount" * 3) tlo)
        ]

mergeOps :: Test
mergeOps =
    TestList
        [ fourWay
            "append"
            (ordersDF <> ordersDF)
            (DT.append ordersT ordersT)
            (lo <> lo)
            (TL.append tlo tlo)
        , fourWay
            "(|||)"
            (D.select ["cid"] ordersDF D.||| D.select ["amount"] ordersDF)
            (DT.select @'["cid"] ordersT DT.||| DT.select @'["amount"] ordersT)
            (L.select ["cid"] lo L.||| L.select ["amount"] lo)
            (TL.select @'["cid"] tlo TL.||| TL.select @'["amount"] tlo)
        , fourWay
            "union"
            (D.union ordersDF (D.take 2 ordersDF))
            (DT.union ordersT (DT.take 2 ordersT))
            (L.union lo (L.take 2 lo))
            (TL.union tlo (TL.take 2 tlo))
        , fourWay
            "intersect"
            (D.intersect ordersDF (D.take 2 ordersDF))
            (DT.intersect ordersT (DT.take 2 ordersT))
            (L.intersect lo (L.take 2 lo))
            (TL.intersect tlo (TL.take 2 tlo))
        , fourWay
            "difference"
            (D.difference ordersDF (D.take 2 ordersDF))
            (DT.difference ordersT (DT.take 2 ordersT))
            (L.difference lo (L.take 2 lo))
            (TL.difference tlo (TL.take 2 tlo))
        , fourWay
            "symmetricDifference"
            (D.symmetricDifference ordersDF (D.drop 3 ordersDF))
            (DT.symmetricDifference ordersT (DT.drop 3 ordersT))
            (L.symmetricDifference lo (L.drop 3 lo))
            (TL.symmetricDifference tlo (TL.drop 3 tlo))
        , fourWay
            "fold"
            (D.fold (\n -> D.filter (F.int "cid" D../=. F.lit n)) [1, 2] ordersDF)
            (DT.fold (\n -> DT.filter (DT.col @"cid" DT../=. DT.lit n)) [1, 2] ordersT)
            (L.fold (\n -> L.filter (F.int "cid" L../=. F.lit n)) [1, 2] lo)
            (TL.fold (\n -> TL.filter (TL.col @"cid" TL../=. TL.lit n)) [1, 2] tlo)
        ]

summaries :: Test
summaries =
    TestList
        [ untypedPair "summarize" (D.summarize ordersDF) (L.summarize lo)
        , untypedPair "summarize (typed)" (DT.summarize ordersT) (TL.summarize tlo)
        , untypedPair
            "describeColumns"
            (D.describeColumns ordersDF)
            (L.describeColumns lo)
        , untypedPair
            "frequencies"
            (D.frequencies (F.text "region") ordersDF)
            (L.frequencies (F.text "region") lo)
        , untypedPair
            "frequencies (typed)"
            (DT.frequencies (DT.col @"region") ordersT)
            (TL.frequencies (TL.col @"region") tlo)
        ]

values :: Test
values =
    TestList
        [ valueFourWay
            "mean"
            (D.mean (F.double "amount") ordersDF)
            (DT.mean (DT.col @"amount") ordersT)
            (L.mean (F.double "amount") lo)
            (TL.mean (TL.col @"amount") tlo)
        , valueFourWay
            "sum"
            (D.sum (F.int "cid") ordersDF)
            (DT.sum (DT.col @"cid") ordersT)
            (L.sum (F.int "cid") lo)
            (TL.sum (TL.col @"cid") tlo)
        , valueFourWay
            "median of a filtered query"
            (D.median (F.double "amount") (D.filter (F.int "cid" D..>. 1) ordersDF))
            (DT.median (DT.col @"amount") (DT.filter (DT.col @"cid" DT..>. 1) ordersT))
            (L.median (F.double "amount") (L.filter (F.int "cid" L..>. 1) lo))
            (TL.median (TL.col @"amount") (TL.filter (TL.col @"cid" TL..>. 1) tlo))
        , valueFourWay
            "dimensions"
            (D.dimensions ordersDF)
            (DT.dimensions ordersT)
            (L.dimensions lo)
            (TL.dimensions tlo)
        , valueFourWay
            "columnNames"
            (D.columnNames ordersDF)
            (DT.columnNames ordersT)
            (L.columnNames lo)
            (TL.columnNames tlo)
        , valueFourWay
            "valueCounts"
            (D.valueCounts (F.text "region") ordersDF)
            (DT.valueCounts (DT.col @"region") ordersT)
            (L.valueCounts (F.text "region") lo)
            (TL.valueCounts (TL.col @"region") tlo)
        , valueFourWay
            "columnAsList"
            (D.columnAsList (F.double "amount") ordersDF)
            (DT.columnAsList @"amount" ordersT)
            (L.columnAsList (F.double "amount") lo)
            (TL.columnAsList @"amount" tlo)
        , valueFourWay
            "correlation"
            (D.correlation "cid" "amount" ordersDF)
            (DT.correlation @"cid" @"amount" ordersT)
            (L.correlation "cid" "amount" lo)
            (TL.correlation @"cid" @"amount" tlo)
        , valueFourWay
            "toCsv"
            (D.toCsv ordersDF)
            (DT.toCsv ordersT)
            (L.toCsv lo)
            (TL.toCsv tlo)
        , valueFourWay
            "null"
            (D.null ordersDF)
            (DT.null ordersT)
            (L.null lo)
            (TL.null tlo)
        ]

tests :: [Test]
tests = [joins, groupAndSort, rowOps, columnOps, mergeOps, summaries, values]
