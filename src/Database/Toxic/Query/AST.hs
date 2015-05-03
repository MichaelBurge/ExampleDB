{-# LANGUAGE DeriveDataTypeable,DeriveGeneric,TemplateHaskell #-}
module Database.Toxic.Query.AST where

import GHC.Generics
import Control.DeepSeq

import Control.Exception
import Control.Lens
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Typeable
import qualified Data.Vector as V
import Database.Toxic.Types

type Condition = Expression

data Literal =
    LBool Bool
  | LInt Integer
  | LNull
  | LValue Value
  deriving (Eq, Show)

data QueryAggregate =
    QAggBoolOr
  | QAggSum
  | QAggFailIfAggregated
  deriving (Eq, Show)

data Unop =
  UnopNot
  deriving (Eq, Show)

data Binop =
    BinopPlus
  | BinopMinus
  | BinopTimes
  | BinopDividedBy
  | BinopGreaterOrEqual
  | BinopGreater
  | BinopEqual
  | BinopLessOrEqual
  | BinopLess
  | BinopUnequal
  deriving (Eq, Show)

data Expression =
    ELiteral Literal
  | ERename Expression T.Text
  | ECase (ArrayOf (Condition, Expression)) (Maybe Expression)
  | EVariable T.Text
  | EAggregate QueryAggregate Expression
  | EPlaceholder Int
  | EUnop Unop Expression
  | EBinop Binop Expression Expression
  deriving (Eq, Show)


data Query = SingleQuery {
  queryGroupBy :: Maybe (ArrayOf Expression),
  queryOrderBy :: Maybe (ArrayOf (Expression, StreamOrder)),
  queryProject :: ArrayOf Expression,
  querySource  :: Maybe Query
  } | SumQuery {
   queryCombineOperation   :: QuerySumOperation,
   queryConstituentQueries :: ArrayOf Query
  } | ProductQuery {
  queryFactors :: ArrayOf Query
  }
  deriving (Eq, Show)

data TableSpec = TableSpec {
  tableSpecColumns :: ArrayOf Column
  } deriving (Eq, Show)

data Table = Table {
  tableName :: T.Text,
  tableSpec :: TableSpec
  } deriving (Eq, Show)

data Environment = Environment {
  _environmentTables :: M.Map T.Text Table
   } deriving (Eq, Show)

data Statement =
    SQuery Query
  | SCreateTable T.Text TableSpec
  deriving (Eq, Show)

data QueryError =
  ErrorParseError T.Text
  | ErrorUnknownVariable T.Text
  deriving (Eq, Show, Typeable, Generic)

instance Exception QueryError
instance NFData QueryError

makeLenses ''Environment

singleton_query :: Expression -> Query
singleton_query expression = SingleQuery {
  queryGroupBy = Nothing,
  queryProject = V.singleton expression,
  querySource = Nothing,
  queryOrderBy = Nothing
  }

singleton_statement :: Expression -> Statement
singleton_statement = SQuery . singleton_query
