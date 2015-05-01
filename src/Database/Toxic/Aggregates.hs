{-# LANGUAGE OverloadedStrings,RankNTypes #-}
module Database.Toxic.Aggregates where

import qualified Data.Text as T
import qualified Data.Vector as V

import Database.Toxic.Operators
import Database.Toxic.Types

bind1 arguments function =
  let numArguments = V.length arguments
  in if numArguments /= 1
     then error $ "Incorrect number of arguments: " ++ show numArguments
     else function $ V.head arguments

simpleFoldAggregate :: T.Text -> (AggregateState -> Value -> Value) -> Value -> AggregateFunction
simpleFoldAggregate name f initial =
  AggregateFunction {
    aggregateInitialize = initial,
    aggregateAccumulate = f,
    aggregateFinalize   = id,
    aggregateName       = name,
    aggregateType       = valueType initial
    }

bool_or :: AggregateFunction
bool_or = simpleFoldAggregate "bool_or" operatorOr $ VBool False

sum :: AggregateFunction
sum = simpleFoldAggregate "sum" operatorPlus $ VInt 0
