{-# LANGUAGE RankNTypes #-}

module Database.Toxic.Types where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Map.Strict as M

type ArrayOf a = V.Vector a
type SetOf a = [ a ]

data StreamOrder = Unordered | Descending | Ascending deriving (Eq, Ord, Show)

data Value =
      VBool Bool
    | VInt Int
    | VNull
    deriving (Eq, Ord, Show)

data Type =
      TBool
    | TInt
    | TUnknown
    deriving (Eq, Ord, Show)

data Column = Column {
  columnName :: T.Text,
  columnType :: Type
  } deriving (Eq, Ord, Show)

newtype Record = Record (ArrayOf Value) deriving (Eq, Ord, Show)
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
newtype AggregateRow = AggregateRow (ArrayOf AggregateState)

type PrimaryKey = Record
type Row = Record

type PendingSummarization = M.Map PrimaryKey AggregateRow
type Summarization = M.Map PrimaryKey Record
