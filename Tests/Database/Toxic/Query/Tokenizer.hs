{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.Query.Tokenizer (tokenizerTests) where

import Prelude hiding (lex)
import qualified Data.Text as T

import Database.Toxic.Query.Tokenizer

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

test_booleanSelect :: Assertion
test_booleanSelect = do
  let statement      = "select true;"
  let actualTokens   = unsafeRunTokenLexer statement
  let expectedTokens = [ TkSelect, TkTrue, TkStatementEnd ]
  assertEqual "Boolean select" expectedTokens actualTokens

tokenizerTests :: Test.Framework.Test
tokenizerTests =
  testGroup "Tokenizer" [
    testCase "Boolean select" test_booleanSelect
    ]
