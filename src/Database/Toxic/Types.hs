{-# LANGUAGE RankNTypes #-}

module Database.Toxic.Types where

import qualified Data.Text as T
import qualified Data.Vector as V

type ArrayOf a = V.Vector a
type SetOf a = [ a ]
  
data Value =
      VBool Bool
    | VNull
    deriving (Eq, Show)

data Type =
      TBool
    | TUnknown
    deriving (Eq, Show)

data Column = Column {
  columnName :: T.Text,
  columnType :: Type
  } deriving (Eq, Show)

data Record = Record (ArrayOf Value) deriving (Eq, Show)
data Stream = Stream {
  streamHeader  :: ArrayOf Column,
  streamRecords :: SetOf Record
  } deriving (Eq, Show)

data Table = Table {
  tableName   :: T.Text,
  tableStream :: Stream
  } deriving (Eq, Show)


data QuerySumOperation =
  QuerySumUnionAll
  deriving (Eq, Show)

data QueryProductOperation =
  QueryProductCrossJoin
  deriving (Eq, Show)

type AggregateState = Value
data AggregateFunction = AggregateFunction {
  aggregateInitialize :: AggregateState,
  aggregateAccumulate :: Value -> AggregateState -> AggregateState,
  aggregateFinalize   :: AggregateState -> Value,
  aggregateName       :: T.Text,
  aggregateType       :: Type
}
