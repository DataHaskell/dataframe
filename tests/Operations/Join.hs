{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Join where

import Data.Text (Text)
import Data.These
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Operations.Join
import qualified DataFrame.Typed as DT
import Test.HUnit

df1 :: D.DataFrame
df1 =
    D.fromNamedColumns
        [ ("key", D.fromList ["K0" :: Text, "K1", "K2", "K3", "K4", "K5"])
        , ("A", D.fromList ["A0" :: Text, "A1", "A2", "A3", "A4", "A5"])
        ]

df2 :: D.DataFrame
df2 =
    D.fromNamedColumns
        [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
        , ("B", D.fromList ["B0" :: Text, "B1", "B2"])
        ]

testInnerJoin :: Test
testInnerJoin =
    TestCase
        ( assertEqual
            "Test inner join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
                , ("A", D.fromList ["A0" :: Text, "A1", "A2"])
                , ("B", D.fromList ["B0" :: Text, "B1", "B2"])
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "key")] (innerJoin ["key"] df1 df2))
        )

testLeftJoin :: Test
testLeftJoin =
    TestCase
        ( assertEqual
            "Test left join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K2", "K3", "K4", "K5"])
                , ("A", D.fromList ["A0" :: Text, "A1", "A2", "A3", "A4", "A5"])
                , ("B", D.fromList [Just "B0", Just "B1" :: Maybe Text, Just "B2"])
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "key")] (leftJoin ["key"] df2 df1))
        )

testRightJoin :: Test
testRightJoin =
    TestCase
        ( assertEqual
            "Test right join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
                , ("A", D.fromList [Just "A0" :: Maybe Text, Just "A1", Just "A2"])
                , ("B", D.fromList ["B0" :: Text, "B1", "B2"])
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "key")] (rightJoin ["key"] df2 df1))
        )

tdf1 :: DT.TypedDataFrame [DT.Column "key" Text, DT.Column "A" Text]
tdf1 = either (error . show) id (DT.freezeWithError df1)

tdf2 :: DT.TypedDataFrame [DT.Column "key" Text, DT.Column "B" Text]
tdf2 = either (error . show) id (DT.freezeWithError df2)

testInnerJoinTyped :: Test
testInnerJoinTyped =
    TestCase
        ( assertEqual
            "Test typed inner join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
                , ("A", D.fromList ["A0" :: Text, "A1", "A2"])
                , ("B", D.fromList ["B0" :: Text, "B1", "B2"])
                ]
            )
            (DT.thaw $ DT.sortBy [DT.asc (DT.col @"key")] (DT.innerJoin @'["key"] tdf1 tdf2))
        )

testLeftJoinTyped :: Test
testLeftJoinTyped =
    TestCase
        ( assertEqual
            "Test typed left join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K2", "K3", "K4", "K5"])
                , ("A", D.fromList ["A0" :: Text, "A1", "A2", "A3", "A4", "A5"])
                , ("B", D.fromList [Just "B0", Just "B1" :: Maybe Text, Just "B2"])
                ]
            )
            (DT.thaw $ DT.sortBy [DT.asc (DT.col @"key")] (DT.leftJoin @'["key"] tdf1 tdf2))
        )

testRightJoinTyped :: Test
testRightJoinTyped =
    TestCase
        ( assertEqual
            "Test typed right join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
                , ("A", D.fromList [Just "A0" :: Maybe Text, Just "A1", Just "A2"])
                , ("B", D.fromList ["B0" :: Text, "B1", "B2"])
                ]
            )
            (DT.thaw $ DT.sortBy [DT.asc (DT.col @"key")] (DT.rightJoin @'["key"] tdf1 tdf2))
        )

staffDf :: D.DataFrame
staffDf =
    D.fromRows
        ["Name", "Role"]
        [ [D.toAny @Text "Kelly", D.toAny @Text "Director of HR"]
        , [D.toAny @Text "Sally", D.toAny @Text "Course liasion"]
        , [D.toAny @Text "James", D.toAny @Text "Grader"]
        ]

studentDf :: D.DataFrame
studentDf =
    D.fromRows
        ["Name", "School"]
        [ [D.toAny @Text "James", D.toAny @Text "Business"]
        , [D.toAny @Text "Mike", D.toAny @Text "Law"]
        , [D.toAny @Text "Sally", D.toAny @Text "Engineering"]
        ]

testFullOuterJoin :: Test
testFullOuterJoin =
    TestCase
        ( assertEqual
            "Test full outer join with single key"
            ( D.fromNamedColumns
                [
                    ( "Name"
                    , D.fromList ["James" :: Text, "Kelly", "Mike", "Sally"]
                    )
                ,
                    ( "Role"
                    , D.fromList
                        [ Just "Grader" :: Maybe Text
                        , Just "Director of HR"
                        , Nothing
                        , Just "Course liasion"
                        ]
                    )
                ,
                    ( "School"
                    , D.fromList
                        [Just "Business" :: Maybe Text, Nothing, Just "Law", Just "Engineering"]
                    )
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "Name")] (fullOuterJoin ["Name"] studentDf staffDf))
        )

dfL :: D.DataFrame
dfL =
    D.fromNamedColumns
        [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
        , ("X", D.fromList ["LX0" :: Text, "LX1", "LX2"])
        , ("Lonly", D.fromList ["L0" :: Text, "L1", "L2"])
        ]

dfR :: D.DataFrame
dfR =
    D.fromNamedColumns
        [ ("key", D.fromList ["K0" :: Text, "K1", "K3"])
        , ("X", D.fromList ["RX0" :: Text, "RX1", "RX3"])
        , ("Ronly", D.fromList [10 :: Int, 11, 13])
        ]

testInnerJoinWithCollisions :: Test
testInnerJoinWithCollisions =
    TestCase
        ( assertEqual
            "Test inner join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1"])
                , ("X", D.fromList [These "LX0" "RX0" :: These Text Text, These "LX1" "RX1"])
                , ("Lonly", D.fromList ["L0" :: Text, "L1"])
                , ("Ronly", D.fromList [10 :: Int, 11])
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "key")] (innerJoin ["key"] dfR dfL))
        )

testLeftJoinWithCollisions :: Test
testLeftJoinWithCollisions =
    TestCase
        ( assertEqual
            "Test left join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
                ,
                    ( "X"
                    , D.fromList [These "LX0" "RX0" :: These Text Text, These "LX1" "RX1", This "LX2"]
                    )
                , ("Lonly", D.fromList ["L0" :: Text, "L1", "L2"])
                , ("Ronly", D.fromList [Just 10 :: Maybe Int, Just 11, Nothing])
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "key")] (leftJoin ["key"] dfR dfL))
        )

testRightJoinWithCollisions :: Test
testRightJoinWithCollisions =
    TestCase
        ( assertEqual
            "Test right join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K3"])
                ,
                    ( "X"
                    , D.fromList [These "RX0" "LX0" :: These Text Text, These "RX1" "LX1", This "RX3"]
                    )
                , ("Ronly", D.fromList [10 :: Int, 11, 13])
                , ("Lonly", D.fromList [Just "L0" :: Maybe Text, Just "L1", Nothing])
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "key")] (rightJoin ["key"] dfR dfL))
        )

testOuterJoinWithCollisions :: Test
testOuterJoinWithCollisions =
    TestCase
        ( assertEqual
            "Test right join with single key"
            ( D.fromNamedColumns
                [ ("key", D.fromList ["K0" :: Text, "K1", "K2", "K3"])
                ,
                    ( "X"
                    , D.fromList
                        [ Just (These "LX0" "RX0") :: Maybe (These Text Text)
                        , Just (These "LX1" "RX1")
                        , Just (This "LX2")
                        , Just (That "RX3")
                        ]
                    )
                , ("Lonly", D.fromList [Just "L0" :: Maybe Text, Just "L1", Just "L2", Nothing])
                , ("Ronly", D.fromList [Just 10 :: Maybe Int, Just 11, Nothing, Just 13])
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "key")] (fullOuterJoin ["key"] dfR dfL))
        )

tests :: [Test]
tests =
    [ TestLabel "innerJoin" testInnerJoin
    , TestLabel "testInnerJoinTyped" testInnerJoinTyped
    , TestLabel "leftJoin" testLeftJoin
    , TestLabel "testLeftJoinTyped" testLeftJoinTyped
    , TestLabel "rightJoin" testRightJoin
    , TestLabel "testRightJoinTyped" testRightJoinTyped
    , TestLabel "fullOuterJoin" testFullOuterJoin
    , TestLabel "innerJoinWithCollisions" testInnerJoinWithCollisions
    , TestLabel "leftJoinWithCollisions" testLeftJoinWithCollisions
    , TestLabel "rightJoinWithCollisions" testRightJoinWithCollisions
    , TestLabel "outerJoinWithCollisions" testOuterJoinWithCollisions
    ]
