module Database.Toxic.Query.AST where

import qualified Data.Text as T

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
