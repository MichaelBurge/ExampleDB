module Database.Toxic.Query.AST where

import Database.Toxic.Types

data Literal =
  LBool Bool
  deriving (Eq, Show)

data Expression =
  ELiteral Literal
  deriving (Eq, Show)

data Query = Query {
  queryProject :: ArrayOf Expression
  } deriving (Eq, Show)

data Statement =
  SQuery Query
  deriving (Eq, Show)
