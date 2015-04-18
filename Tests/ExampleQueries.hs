{-# LANGUAGE OverloadedStrings #-}

module Tests.ExampleQueries (exampleQueriesTests) where

import Database.Toxic.Types
import Database.Toxic.Query.AST
import Database.Toxic.Query.Interpreter
import Database.Toxic.Query.Parser
import Database.Toxic.Query.Tokenizer
import qualified Data.Text as T
import qualified Data.Vector as V

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

assertQueryResults :: Environment -> T.Text -> Stream -> Assertion
assertQueryResults environment query expectedStream = do
  let tokens = unsafeRunTokenLexer query
  let statement = unsafeRunTokenParser tokens
  actualStream <- execute environment statement
  assertEqual "assertQueryResults" expectedStream actualStream

test_booleanSelect :: Assertion
test_booleanSelect = do
  let query = "select true;"
  let expectedColumn =  Column { columnName = "literal", columnType = TBool }
  let expectedStream = singleton_stream expectedColumn $ VBool True
  assertQueryResults nullEnvironment query expectedStream

test_rename :: Assertion
test_rename= do
  let query = "select true as example;"
  let expectedColumn = Column { columnName = "example", columnType = TBool }
  let expectedStream = singleton_stream expectedColumn $ VBool True
  assertQueryResults nullEnvironment query expectedStream

test_case_when_value :: Assertion
test_case_when_value =
  let query = "select case when true then false end;"
      expectedColumn = Column { columnName = "case", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool False
  in assertQueryResults nullEnvironment query expectedStream

test_case_when_null :: Assertion
test_case_when_null =
  let query = "select case when false then false end;"
      expectedColumn = Column { columnName = "case", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VNull
  in assertQueryResults nullEnvironment query expectedStream

test_case_when_else :: Assertion
test_case_when_else =
  let query = "select case when false then false else true end;"
      expectedColumn = Column { columnName = "case", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in assertQueryResults nullEnvironment query expectedStream

test_case_when_when :: Assertion
test_case_when_when =
  let query = "select case when false then false when true then true else false end;"
      expectedColumn = Column { columnName = "case", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in assertQueryResults nullEnvironment query expectedStream

test_union :: Assertion
test_union =
  let query = "select true union select false;"
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn [ VBool True, VBool False ]
  in assertQueryResults nullEnvironment query expectedStream

exampleQueriesTests :: Test.Framework.Test
exampleQueriesTests =
  testGroup "Example queries" [
    testCase "Boolean select" test_booleanSelect,
    testCase "Rename" test_rename,
    testCase "Case when(value)" test_case_when_value,
    testCase "Case when(null)" test_case_when_null,
    testCase "Case when else" test_case_when_else,
    testCase "Case when when" test_case_when_when,
    testCase "Union" test_union
    ]
