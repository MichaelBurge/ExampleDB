{-# LANGUAGE OverloadedStrings #-}
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

accumulate1 :: (Value -> Value -> Value) -> Value -> ArrayOf Value -> Value
accumulate1 f x arguments = bind1 arguments $ \y ->
  f x y

simpleFoldAggregate :: T.Text -> (Value -> Value -> Value) -> Value -> AggregateFunction Value
simpleFoldAggregate name f initial =
  AggregateFunction {
    aggregateInitialize = initial,
    aggregateAccumulate = accumulate1 f,
    aggregateFinalize   = id,
    aggregateName       = name,
    aggregateType       = valueType initial
    }

bool_or :: AggregateFunction Value
bool_or = simpleFoldAggregate "bool_or" operatorOr $ VBool False
