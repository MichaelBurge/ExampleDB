module Database.Toxic.TSql.AST where

import qualified Data.ByteString as BS

import Database.Toxic.Query.AST

data Command =
  CStatement BS.ByteString
  deriving (Eq, Show)
