module Database.Toxic.Query.AST where

import qualified Data.Text as T
import qualified Data.Vector as V
import Database.Toxic.Types

type Condition = Expression

data Literal =
  LBool Bool
  deriving (Eq, Show)

data Expression =
    ELiteral Literal
  | ERename Expression T.Text
  | ECase (ArrayOf (Condition, Expression)) (Maybe Expression)
  deriving (Eq, Show)

data Query = Query {
  queryProject :: ArrayOf Expression
  } deriving (Eq, Show)

data Statement =
  SQuery Query
  deriving (Eq, Show)


singleton_statement :: Expression -> Statement
singleton_statement expression = SQuery $ Query {
  queryProject = V.singleton expression
  }

singleton_stream :: Column -> Value -> Stream
singleton_stream column value =
  let header = V.singleton column
      records = [ Record $ V.singleton value ]
  in Stream { streamHeader = header, streamRecords = records }
