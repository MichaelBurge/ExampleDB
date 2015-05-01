module Database.Toxic.TSql.AST where

import Database.Toxic.Query.AST

data Command =
  CStatement Statement
  deriving (Eq, Show)
