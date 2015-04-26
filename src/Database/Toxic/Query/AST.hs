module Database.Toxic.Query.AST where

import qualified Data.Text as T
import qualified Data.Vector as V
import Database.Toxic.Types

type Condition = Expression

data Literal =
    LBool Bool
  | LNull
  | LValue Value
  deriving (Eq, Show)

data QueryAggregate =
    QAggBoolOr
  | QAggFailIfAggregated
  deriving (Eq, Show)

data Expression =
    ELiteral Literal
  | ERename Expression T.Text
  | ECase (ArrayOf (Condition, Expression)) (Maybe Expression)
  | EVariable T.Text
  | EAggregate QueryAggregate Expression
  | EPlaceholder Int
  deriving (Eq, Show)


data Query = SingleQuery {
  queryGroupBy :: Maybe (ArrayOf Expression),
  queryProject :: ArrayOf Expression,
  querySource  :: Maybe Query
  } | SumQuery {
   queryCombineOperation   :: QuerySumOperation,
   queryConstituentQueries :: ArrayOf Query
  } | ProductQuery {
  queryFactors :: ArrayOf Query
  }
  deriving (Eq, Show)
data Statement =
  SQuery Query
  deriving (Eq, Show)


singleton_statement :: Expression -> Statement
singleton_statement expression = SQuery $ SingleQuery {
  queryGroupBy = Nothing,
  queryProject = V.singleton expression,
  querySource = Nothing
  }
