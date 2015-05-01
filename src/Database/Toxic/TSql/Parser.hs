{-# LANGUAGE OverloadedStrings #-}

module Database.Toxic.TSql.Parser where

import Control.Applicative
import qualified Data.Text as T
import Text.Parsec

import Database.Toxic.Query.AST
import Database.Toxic.Query.Parser
import Database.Toxic.TSql.AST

command :: CharParser Command
command = CStatement <$> statement

runCommandParser :: T.Text -> Either ParseError Command
runCommandParser text = parse command "runCommandParser" $ T.unpack text

unsafeRunCommandParser :: T.Text -> Command
unsafeRunCommandParser text = case runCommandParser text of
  Left parseError -> error $ show parseError
  Right statement -> statement
