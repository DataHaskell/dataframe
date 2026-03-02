{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Typed.Freeze (
    -- * Safe boundary
    freeze,
    freezeWithError,

    -- * Escape hatches
    thaw,
    unsafeFreeze,
) where

import qualified Data.Text as T
import Type.Reflection (SomeTypeRep)

import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Operations.Core (columnNames)
import DataFrame.Typed.Schema (KnownSchema (..))
import DataFrame.Typed.Types (TypedDataFrame (..))

{- | Validate that an untyped 'DataFrame' matches the expected schema @cols@,
then wrap it. Returns 'Nothing' on mismatch.
-}
freeze ::
    forall cols. (KnownSchema cols) => D.DataFrame -> Maybe (TypedDataFrame cols)
freeze df = case validateSchema @cols df of
    Left _ -> Nothing
    Right _ -> Just (TDF df)

-- | Like 'freeze' but returns a descriptive error message on failure.
freezeWithError ::
    forall cols.
    (KnownSchema cols) =>
    D.DataFrame -> Either T.Text (TypedDataFrame cols)
freezeWithError df = case validateSchema @cols df of
    Left err -> Left err
    Right _ -> Right (TDF df)

{- | Unwrap a typed DataFrame back to the untyped representation.
Always safe; discards type information.
-}
thaw :: TypedDataFrame cols -> D.DataFrame
thaw (TDF df) = df

{- | Wrap an untyped DataFrame without any validation.
Used internally after delegation where the library guarantees schema correctness.
-}
unsafeFreeze :: D.DataFrame -> TypedDataFrame cols
unsafeFreeze = TDF

validateSchema ::
    forall cols.
    (KnownSchema cols) =>
    D.DataFrame -> Either T.Text ()
validateSchema df = mapM_ checkCol (schemaEvidence @cols)
  where
    checkCol :: (T.Text, SomeTypeRep) -> Either T.Text ()
    checkCol (name, expectedRep) = case D.getColumn name df of
        Nothing ->
            Left $
                "Column '"
                    <> name
                    <> "' not found in DataFrame. "
                    <> "Available columns: "
                    <> T.pack (show (columnNames df))
        Just col ->
            if matchesType expectedRep col
                then Right ()
                else
                    Left $
                        "Type mismatch on column '"
                            <> name
                            <> "': expected "
                            <> T.pack (show expectedRep)
                            <> ", got "
                            <> T.pack (C.columnTypeString col)

-- | Check if a Column's element type matches the expected SomeTypeRep.
matchesType :: SomeTypeRep -> C.Column -> Bool
matchesType expected col = T.pack (show expected) == T.pack (C.columnTypeString col)
