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

data QueryCombineOperation =
  QueryCombineUnion
  deriving (Eq, Show)

data Query = SingleQuery {
  queryProject :: ArrayOf Expression
  } | CompositeQuery {
   queryCombineOperation   :: QueryCombineOperation,
   queryConstituentQueries :: ArrayOf Query
  } deriving (Eq, Show)

data Statement =
  SQuery Query
  deriving (Eq, Show)


singleton_statement :: Expression -> Statement
singleton_statement expression = SQuery $ SingleQuery {
  queryProject = V.singleton expression
  }

singleton_stream :: Column -> Value -> Stream
singleton_stream column value =
  let header = V.singleton column
      records = [ Record $ V.singleton value ]
  in Stream { streamHeader = header, streamRecords = records }

single_column_stream :: Column -> [ Value ] -> Stream
single_column_stream column values =
  let header = V.singleton column
      records = [ Record $ V.fromList values ]
  in Stream { streamHeader = header, streamRecords = records }
