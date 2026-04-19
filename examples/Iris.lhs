Iris Classification with Neural Networks in Haskell
===================================================

This program demonstrates how to build a multiclass classification model
in Haskell using the DataFrame and Hasktorch libraries. It's a direct port
of the PyTorch tutorial found at:
https://machinelearningmastery.com/building-a-multiclass-classification-model-in-pytorch/

What This Program Does
----------------------

1. Loads the famous Iris dataset (flower measurements and species)
2. Splits the data into training (70%) and test (30%) sets
3. Builds a 3-layer neural network (4 inputs → 8 hidden → 3 outputs)
4. Trains the model for 10,000 epochs using gradient descent
5. Evaluates performance with confusion matrices and classification metrics

Language Extensions and Module Setup
-------------------------------------

Haskell allows us to enable certain language features using pragmas.
Think of these like compiler flags in C++ or decorator syntax in Python:

> {-# LANGUAGE DeriveGeneric #-}
> {-# LANGUAGE MultiParamTypeClasses #-}
> {-# LANGUAGE OverloadedStrings #-}
> {-# LANGUAGE RecordWildCards #-}
> {-# LANGUAGE TypeApplications #-}
>
> module Iris (run) where

Now we import the libraries we need. This is similar to `import` statements
in Python or `#include` in C++:

>
> import Control.Exception (throw)
> import Control.Monad (when, zipWithM_)
> import Data.Either
> import Data.Function (on)
> import Data.List (maximumBy)
>
> import qualified Data.Array as A
>
> import qualified Data.Text as T
> import qualified Data.Vector as V
> import qualified Data.Vector.Unboxed as VU
> import GHC.Generics

DataFrame is a Haskell library similar to pandas in Python:

> import DataFrame.Operators
> import qualified DataFrame as D
> import qualified DataFrame.Functions as F
> import qualified DataFrame.Hasktorch as DHT

Hasktorch is a Haskell binding to Torch:

> import Text.Printf (printf)
> import qualified Torch as HT
> import qualified System.Random as SysRand


Defining Our Data Types
------------------------

In Haskell, we can use the type system to represent our data precisely.
The Iris dataset contains three species of flowers, which we represent
as an algebraic data type (similar to an enum in other languages):

> data Iris
>     = Setosa
>     | Versicolor
>     | Virginica
>     deriving (Eq, Show, Read, Ord, Enum)

The `deriving` clause automatically generates useful functions:
- `Eq`: Allows us to compare Iris values for equality
- `Show`: Converts Iris to a String (e.g., "Setosa")
- `Read`: Converts a String to Iris (e.g., "Setosa" → Setosa)
- `Ord`: Allows ordering/sorting
- `Enum`: Lets us convert to/from integers (Setosa=0, Versicolor=1, Virginica=2)


Neural Network Architecture
----------------------------

We define our Multi-Layer Perceptron (MLP) architecture in two parts:

First, a specification that describes the shape of our network:

> data MLPSpec = MLPSpec
>     { inputFeatures :: Int   -- Number of input features (4 for iris)
>     , hiddenFeatures :: Int  -- Number of neurons in hidden layer
>     , outputFeatures :: Int  -- Number of output classes (3 species)
>     }
>     deriving (Show, Eq)

Second, the actual model with its layers. Each layer is a `Linear`
transformation (like `nn.Linear` in PyTorch):

> data MLP = MLP
>     { l0 :: HT.Linear  -- Input → Hidden layer
>     , l1 :: HT.Linear  -- Hidden → Output layer
>     }
>     deriving (Generic, Show)

Network Architecture Diagram:

    Input Layer (4)  →  Hidden Layer (8)  →  Output Layer (3)
    ---------------     -----------------     ----------------
    sepal.length        ReLU activation       Softmax
    sepal.width         (introduces           (produces
    petal.length        non-linearity)        probabilities)
    petal.width                               Setosa
                                              Versicolor
                                              Virginica

Making Our Model Trainable
---------------------------

We need to tell Hasktorch how to initialize our network with random weights.
This is similar to defining `__init__()` in a PyTorch `nn.Module`:

> instance HT.Parameterized MLP
> instance HT.Randomizable MLPSpec MLP where
>     sample MLPSpec{..} =
>         MLP
>             <$> HT.sample (HT.LinearSpec inputFeatures hiddenFeatures)
>             <*> HT.sample (HT.LinearSpec hiddenFeatures outputFeatures)

The `<$>` and `<*>` operators are Haskell's way of working with random
initialization. Think of this as: "Create an MLP by randomly sampling
weights for both layers."


Forward Pass
------------

This function defines how data flows through the network. It's equivalent
to the `forward()` method in PyTorch. Read it from right to left (or
bottom to top in the chain):

> mlp :: MLP -> HT.Tensor -> HT.Tensor
> mlp MLP{..} =
>     HT.softmax (HT.Dim 1)      -- 4. Apply softmax (probabilities sum to 1)
>         . HT.linear l1          -- 3. Apply second linear layer
>         . HT.relu               -- 2. Apply ReLU activation
>         . HT.linear l0          -- 1. Apply first linear layer

In Python/PyTorch, this would look like:
```python
def forward(self, x):
    x = self.l0(x)
    x = F.relu(x)
    x = self.l1(x)
    x = F.softmax(x, dim=1)
    return x
```


Training Loop
-------------

This is our main training function. It's similar to the epoch loop in
PyTorch training code:

> trainLoop ::
>     Int ->                          -- Number of epochs
>     (HT.Tensor, HT.Tensor) ->      -- Training features and labels
>     (HT.Tensor, HT.Tensor) ->      -- Test features and labels
>     MLP ->                          -- Initial model
>     IO MLP                          -- Returns trained model
> trainLoop
>     n
>     (features, labels)
>     (testFeatures, testLabels)
>     initialM =
>         HT.foldLoop initialM n $ \state i -> do
>             -- Forward pass: compute predictions
>             let predicted = mlp state features
>             
>             -- Compute loss (how wrong our predictions are)
>             let loss = HT.binaryCrossEntropyLoss' labels predicted
>             
>             -- Every 1000 iterations, print progress
>             when (i `mod` 1000 == 0) $ do
>                 let testPredicted = mlp state testFeatures
>                 let testLoss = HT.binaryCrossEntropyLoss' testLabels testPredicted
>                 putStrLn $
>                     "Iteration :"
>                         ++ show i
>                         ++ " | Training Set Loss: "
>                         ++ show (HT.asValue loss :: Float)
>                         ++ " | Test Set Loss: "
>                         ++ show (HT.asValue testLoss :: Float)
>             
>             -- Backward pass: update weights using gradient descent
>             -- HT.GD is the optimizer, 1e-2 is the learning rate
>             (state', _) <- HT.runStep state HT.GD loss 1e-2
>             pure state'


Evaluation Metrics
------------------

We define a confusion matrix type to track our predictions:

> type ConfusionMatrix = A.Array (Int, Int) Float

The confusion matrix shows actual vs predicted labels:

                Predicted
              0     1     2
    Actual 0  TP    FN    FN
           1  FP    TP    FN
           2  FP    FP    TP

where rows are actual labels and columns are predicted labels.

> confusionMatrix :: Int -> [Int] -> [Int] -> ConfusionMatrix
> confusionMatrix n actuals preds = A.accumArray (+) 0 bnds [(x, 1) | x <- zip actuals preds]
>   where
>     bnds = ((0, 0), (n - 1, n - 1))

Helper function to print the confusion matrix in a readable format:

> pprintMatrix :: ConfusionMatrix -> String
> pprintMatrix mtx =
>     unlines $
>         header
>             : [ unwords $ printf "%5d" y : [printf "%5.2f" (mtx A.! (x, y)) | x <- [x1 .. x2]]
>               | y <- [y1 .. y2]
>               ]
>   where
>     ((x1, y1), (x2, y2)) = A.bounds mtx
>     header = unwords $ "     " : [printf "%5d" x | x <- [x1 .. x2]]

Calculate sums for precision and recall:

> rowSumAt :: ConfusionMatrix -> Int -> Float
> rowSumAt mtx n = sum [x | ((i, j), x) <- A.assocs mtx, j == n]
>
> colSumAt :: ConfusionMatrix -> Int -> Float
> colSumAt mtx n = sum [x | ((i, j), x) <- A.assocs mtx, i == n]

Precision = True Positives / (True Positives + False Positives)
(Of all the times we predicted class X, how often were we right?)

> classwisePrecision :: ConfusionMatrix -> [Float]
> classwisePrecision mtx = [mtx A.! (i, i) / mtx `rowSumAt` i | i <- [y1 .. y2]]
>   where
>     ((x1, y1), (x2, y2)) = A.bounds mtx

Recall = True Positives / (True Positives + False Negatives)
(Of all the actual class X examples, how many did we find?)

> classwiseRecall :: ConfusionMatrix -> [Float]
> classwiseRecall mtx = [mtx A.! (i, i) / mtx `colSumAt` i | i <- [x1 .. x2]]
>   where
>     ((x1, y1), (x2, y2)) = A.bounds mtx

Convert one-hot encoded predictions back to class labels.
For example: [0.1, 0.8, 0.1] → 1 (because index 1 has highest value)

> reverseOneHot :: HT.Tensor -> [Int]
> reverseOneHot tsr = map (fst . maximumBy (compare `on` snd) . zip [0 ..]) vals
>   where
>     vals = HT.asValue tsr :: [[Float]]


Main Program
------------

Now we bring it all together! The `do` keyword starts a sequence of operations.
Think of this like the `if __name__ == "__main__":` block in Python:

> run :: IO ()
> run = do
>     -- Step 1: Load the dataset
>     -- ========================
>     df <- D.readParquet "../data/iris.parquet"

The iris dataset has 5 columns:
- sepal.length (Double)
- sepal.width (Double)
- petal.length (Double)
- petal.width (Double)
- variety (Text: "Setosa", "Versicolor", or "Virginica")

Convert the text labels to integers using our Iris type:

>     let derivedDf =
>             df
>                 |> D.derive
>                     "variety"
>                     (F.lift (fromEnum . read @Iris . T.unpack) (F.col "variety"))

The `|>` operator pipes data left-to-right (like Unix pipes or method chaining).
This converts: "Setosa" → 0, "Versicolor" → 1, "Virginica" → 2

Step 2: Split into training and test sets
==========================================

> 
>     let (trainDf, testDf) = D.randomSplit (SysRand.mkStdGen 42) 0.7 derivedDf

This is like `train_test_split` in scikit-learn. We use 70% for training
and 30% for testing, with a fixed random seed (42) for reproducibility.

Step 3: Prepare features (X) and labels (y)
============================================

Extract the four measurement columns as our features:

>     let trainFeaturesTr =
>             trainDf
>                 |> D.select ["sepal.length", "sepal.width", "petal.length", "petal.width"]
>                 |> DHT.toTensor
>     let testFeaturesTr =
>             testDf
>                 |> D.select ["sepal.length", "sepal.width", "petal.length", "petal.width"]
>                 |> DHT.toTensor

Extract the labels (species) as integers:

>     let trainLabels = either throw id (D.columnAsIntVector (F.col @Int "variety") trainDf)
>     let testLabels = either throw id (D.columnAsIntVector (F.col @Int "variety") testDf)

Convert labels to one-hot encoding for neural network training:
- 0 (Setosa) → [1.0, 0.0, 0.0]
- 1 (Versicolor) → [0.0, 1.0, 0.0]
- 2 (Virginica) → [0.0, 0.0, 1.0]

>     let trainLabelsTr = HT.toType HT.Float $ HT.oneHot 3 $ HT.asTensor trainLabels
>     let testLabelsTr = HT.toType HT.Float $ HT.oneHot 3 $ HT.asTensor testLabels

Step 4: Initialize the neural network
======================================

Create a random initial model with:
- 4 input neurons (one for each feature)
- 8 hidden neurons (arbitrary choice, can be tuned)
- 3 output neurons (one for each species)

>     initialModel <- HT.sample $ MLPSpec 4 8 3

Step 5: Train the model
========================

Run 10,000 training iterations (epochs):

>     trainedModel <-
>         trainLoop
>             10000
>             (trainFeaturesTr, trainLabelsTr)
>             (testFeaturesTr, testLabelsTr)
>             initialModel
>
>     putStrLn "Your model weights are given as follows: "
>     print trainedModel

Step 6: Evaluate on training set
=================================

>     putStrLn "....................................."
>     putStrLn "....................................."
>     putStrLn "Training Set Summary is as follows: "
>     let predTrain = reverseOneHot $ mlp trainedModel trainFeaturesTr
>     putStrLn "====== Confusion Matrix ========"
>     let confusionTrain = confusionMatrix 3 (VU.toList trainLabels) predTrain
>     putStrLn $ pprintMatrix confusionTrain
>     putStrLn "=========== Classwise Metrics ============="
>     print $ D.fromNamedColumns
>         [ ("variety" , D.fromList (map (toEnum @Iris) [0 .. 2]))
>         , ("precision", D.fromList (classwisePrecision confusionTrain))
>         , ("recall", D.fromList (classwiseRecall confusionTrain))]

Step 7: Evaluate on test set
=============================

This is the true test of our model - how well does it perform on data
it has never seen before?

>     putStrLn "....................................."
>     putStrLn "....................................."
>     putStrLn "Test Set Summary is as follows: "
>     let predTest = reverseOneHot $ mlp trainedModel testFeaturesTr
>     putStrLn "====== Confusion Matrix ========"
>     let confusionTest = confusionMatrix 3 (VU.toList testLabels) predTest
>     putStrLn $ pprintMatrix confusionTest
>     putStrLn "=========== Classwise Metrics ============="
>     print $ D.fromNamedColumns
>         [ ("variety" , D.fromList (map (toEnum @Iris) [0 .. 2]))
>         , ("precision", D.fromList (classwisePrecision confusionTest))
>         , ("recall", D.fromList (classwiseRecall confusionTest))]

Conclusion
==========

This program demonstrates that Haskell can be used for machine learning
tasks just like Python! The strong type system helps catch errors at
compile time, and the functional style leads to concise, composable code.
