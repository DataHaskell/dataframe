{-# LANGUAGE OverloadedStrings #-}

{- |
Regression tests for null-aware row hashing and row extraction.

`Nothing` in a nullable unboxed column must be a distinct value from any
`Just x` (so @distinct@/@groupBy@/joins do not merge them), and `toRowList`
must round-trip nulls faithfully. These pin the bug surfaced by the
category-theory set-algebra laws, where @Nothing@ was stored as an
uninitialised int and collided with @Just 0@.
-}
module Operations.NullableHashing where

import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Row (toRowList)

import Test.HUnit

maybeIntCol :: [Maybe Int] -> D.DataFrame
maybeIntCol xs = D.fromNamedColumns [("k", DI.fromList xs)]

-- | distinct must keep @Nothing@ and @Just 0@ as two distinct rows.
distinctSeparatesNullFromZero :: Test
distinctSeparatesNullFromZero =
    TestCase
        ( assertEqual
            "distinct keeps Nothing and Just 0 apart"
            2
            (fst (D.dimensions (D.distinct (maybeIntCol [Just 0, Nothing, Just 0, Nothing]))))
        )

-- | A whole row that differs only by null-vs-Just-0 must survive distinct.
distinctSeparatesNullRow :: Test
distinctSeparatesNullRow =
    TestCase
        ( assertEqual
            "distinct keeps rows differing only by null vs Just 0"
            2
            ( fst
                ( D.dimensions
                    ( D.distinct
                        ( D.fromNamedColumns
                            [ ("k", DI.fromList [Just (0 :: Int), Nothing])
                            , ("v", DI.fromList ['a', 'a'])
                            ]
                        )
                    )
                )
            )
        )

-- | An inner join on a nullable key: @Just 0@ must not match @Nothing@.
joinNullDoesNotMatchZero :: Test
joinNullDoesNotMatchZero =
    TestCase
        ( assertEqual
            "Just 0 key does not join with Nothing key"
            0
            ( fst
                (D.dimensions (D.innerJoin ["k"] (maybeIntCol [Just 0]) (maybeIntCol [Nothing])))
            )
        )

-- | An inner join on a nullable key: @Nothing@ matches @Nothing@.
joinNullMatchesNull :: Test
joinNullMatchesNull =
    TestCase
        ( assertEqual
            "Nothing key joins with Nothing key"
            1
            ( fst
                (D.dimensions (D.innerJoin ["k"] (maybeIntCol [Nothing]) (maybeIntCol [Nothing])))
            )
        )

-- | toRowList round-trips a nullable column, preserving @Nothing@.
toRowListRoundTripPreservesNull :: Test
toRowListRoundTripPreservesNull =
    let df = maybeIntCol [Just 1, Nothing, Just 3]
        rebuilt = D.fromRows ["k"] (map (map snd) (toRowList df))
     in TestCase (assertEqual "toRowList round-trip preserves nulls" df rebuilt)

{- | Hash robustness: @distinct@ must preserve every row of a dense grid of
small integers across several columns. A weak per-step hash collides badly on
adjacent integers and merges distinct rows; grouping trusts hash equality, so
this guards that the hash spreads such keys.
-}
denseIntGridNoCollisions :: Test
denseIntGridNoCollisions =
    let d = [-4 .. 4 :: Int]
        rows = [(a, b, c) | a <- d, b <- d, c <- d]
        df =
            D.fromNamedColumns
                [ ("a", DI.fromList (map (\(a, _, _) -> a) rows))
                , ("b", DI.fromList (map (\(_, b, _) -> b) rows))
                , ("c", DI.fromList (map (\(_, _, c) -> c) rows))
                ]
     in TestCase
            ( assertEqual
                "distinct preserves a dense 9x9x9 int grid (no hash collisions)"
                (length rows)
                (fst (D.dimensions (D.distinct df)))
            )

{- | Join-hash robustness: a self inner-join on a dense grid of unique integer
keys must return exactly one match per row. Joins match purely by hash, so a
weak hash that collides distinct keys would emit spurious cross-matches.
-}
joinDenseGridNoCollisions :: Test
joinDenseGridNoCollisions =
    let d = [-4 .. 4 :: Int]
        rows = [(a, b) | a <- d, b <- d]
        df =
            D.fromNamedColumns
                [ ("a", DI.fromList (map fst rows))
                , ("b", DI.fromList (map snd rows))
                ]
     in TestCase
            ( assertEqual
                "self inner-join on unique keys has no spurious hash matches"
                (length rows)
                (fst (D.dimensions (D.innerJoin ["a", "b"] df df)))
            )

-- | The same robustness check including @Nothing@ (a nullable grid).
denseNullableGridNoCollisions :: Test
denseNullableGridNoCollisions =
    let d = Nothing : map Just [-3 .. 3 :: Int]
        rows = [(a, b) | a <- d, b <- d]
        df =
            D.fromNamedColumns
                [ ("a", DI.fromList (map fst rows))
                , ("b", DI.fromList (map snd rows))
                ]
     in TestCase
            ( assertEqual
                "distinct preserves a dense nullable grid (no hash collisions)"
                (length rows)
                (fst (D.dimensions (D.distinct df)))
            )

tests :: [Test]
tests =
    [ TestLabel "distinctSeparatesNullFromZero" distinctSeparatesNullFromZero
    , TestLabel "denseIntGridNoCollisions" denseIntGridNoCollisions
    , TestLabel "joinDenseGridNoCollisions" joinDenseGridNoCollisions
    , TestLabel "denseNullableGridNoCollisions" denseNullableGridNoCollisions
    , TestLabel "distinctSeparatesNullRow" distinctSeparatesNullRow
    , TestLabel "joinNullDoesNotMatchZero" joinNullDoesNotMatchZero
    , TestLabel "joinNullMatchesNull" joinNullMatchesNull
    , TestLabel "toRowListRoundTripPreservesNull" toRowListRoundTripPreservesNull
    ]
