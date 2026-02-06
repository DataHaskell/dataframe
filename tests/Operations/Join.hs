{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Join where

import Data.Text (Text)
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Operations.Join
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
                , ("A", D.fromList ["A0" :: Text, "A1", "A2"])
                , ("B", D.fromList ["B0" :: Text, "B1", "B2"])
                ]
            )
            (D.sortBy [D.Asc (F.col @Text "key")] (rightJoin ["key"] df2 df1))
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

tests :: [Test]
tests =
    [ TestLabel "innerJoin" testInnerJoin
    , TestLabel "leftJoin" testLeftJoin
    , TestLabel "rightJoin" testRightJoin
    , TestLabel "fullOuterJoin" testFullOuterJoin
    ]
