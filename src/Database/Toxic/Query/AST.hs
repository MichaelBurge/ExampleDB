module Database.Toxic.Query.AST where

import Database.Toxic.Types

data Literal =
  LBool Bool

data Expression =
  ELiteral Literal

data Query = Query {
  queryProject :: ArrayOf Expression
                   }

data Statement =
  SQuery Query
