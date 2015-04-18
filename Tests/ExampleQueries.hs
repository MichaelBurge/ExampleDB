{-# LANGUAGE OverloadedStrings #-}

module Tests.ExampleQueries (exampleQueriesTests) where

import Database.Toxic.Types
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
  let expectedHeader = V.singleton $
                       Column { columnName = "literal", columnType = TBool }
  let expectedRecords = [ Record $ V.singleton $ VBool True ]
  let expectedStream = Stream {
        streamHeader = expectedHeader, streamRecords = expectedRecords
        }
  assertQueryResults nullEnvironment query expectedStream

exampleQueriesTests :: Test.Framework.Test
exampleQueriesTests =
  testGroup "Example queries" [
    testCase "Boolean select" test_booleanSelect
    ]
