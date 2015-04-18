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

assert_tokenizes :: T.Text -> [ Token ] -> Assertion
assert_tokenizes statement expectedTokens = do
  let actualTokens   = unsafeRunTokenLexer statement
  assertEqual "" expectedTokens actualTokens

test_booleanSelect :: Assertion
test_booleanSelect = do
  assert_tokenizes "select true;" [ TkSelect, TkTrue, TkStatementEnd ]

test_rename :: Assertion
test_rename = do
  assert_tokenizes "select true as example" [
    TkSelect, TkTrue, TkRename, TkIdentifier "example"
    ]

test_case_when :: Assertion
test_case_when = do
  assert_tokenizes "select case when true then true end" [
    TkSelect, TkCase, TkWhen, TkTrue, TkThen, TkTrue, TkEnd
    ]

test_case_when_else :: Assertion
test_case_when_else = do
  assert_tokenizes "select case when true then true else false end" [
    TkSelect, TkCase, TkWhen, TkTrue, TkThen, TkTrue, TkElse, TkFalse, TkEnd
    ]

test_case_when_when_else :: Assertion
test_case_when_when_else =
  assert_tokenizes "select case when false then true when true then false else false end" [
    TkSelect, TkCase, TkWhen, TkFalse, TkThen, TkTrue,
                      TkWhen, TkTrue,  TkThen, TkFalse,
                      TkElse, TkFalse,
               TkEnd
    ]

tokenizerTests :: Test.Framework.Test
tokenizerTests =
  testGroup "Tokenizer" [
    testCase "Boolean select" test_booleanSelect,
    testCase "Rename expression" test_rename,
    testCase "Case when" test_case_when,
    testCase "Case when else" test_case_when_else,
    testCase "Case when when else" test_case_when_when_else
    ]
