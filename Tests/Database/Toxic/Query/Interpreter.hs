{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.Query.Interpreter (interpreterTests) where

import qualified Data.Vector as V

import Database.Toxic.Types
import Database.Toxic.Query.AST
import Database.Toxic.Query.Interpreter

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

test_booleanSelect :: Assertion
test_booleanSelect = do
  let expression = ELiteral $ LBool True
  let statement = singleton_statement expression
  actualStream <- execute nullEnvironment statement
  let expectedColumn = Column {
        columnName = "literal",
        columnType = TBool
        }
  let expectedStream = singleton_stream expectedColumn $ VBool True
  assertEqual "Boolean select" expectedStream actualStream

test_rename :: Assertion
test_rename = do
  let expression = ERename (ELiteral $ LBool True) "example"
  let statement  = singleton_statement expression
  actualStream <- execute nullEnvironment statement
  let expectedColumn = Column {
        columnName = "example",
        columnType = TBool
        }
  let expectedStream = singleton_stream expectedColumn $ VBool True
  assertEqual "Rename" expectedStream actualStream

test_case_when_value :: Assertion
test_case_when_value =
  let condition  = (ELiteral $ LBool True, ELiteral $ LBool True)
      expression = ECase (V.singleton condition) Nothing
      statement  = singleton_statement expression
      expectedColumn = Column {
        columnName = "case",
        columnType = TBool
        }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "Case when(value)" expectedStream actualStream

test_case_when_null :: Assertion
test_case_when_null =
  let condition  = (ELiteral $ LBool False, ELiteral $ LBool True)
      expression = ECase (V.singleton condition) Nothing
      statement  = singleton_statement expression
      expectedColumn = Column {
        columnName = "case",
        columnType = TBool
        }
      expectedStream = singleton_stream expectedColumn $ VNull
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "Case when(null)" expectedStream actualStream

test_case_when_else :: Assertion
test_case_when_else =
  let condition  = (ELiteral $ LBool False, ELiteral $ LBool True)
      otherwise  = Just $ ELiteral $ LBool False
      expression = ECase (V.singleton condition) otherwise
      statement  = singleton_statement expression
      expectedColumn = Column {
        columnName = "case",
        columnType = TBool
        }
      expectedStream = singleton_stream expectedColumn $ VBool False
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "Case when when" expectedStream actualStream

test_case_when_when :: Assertion
test_case_when_when =
  let condition1 = (ELiteral $ LBool False, ELiteral $ LBool True)
      condition2 =(ELiteral $ LBool True, ELiteral $ LBool False)
      otherwise  = Just $ ELiteral $ LBool True
      expression = ECase (V.fromList [condition1,condition2]) otherwise
      statement  = singleton_statement expression
      expectedColumn = Column {
        columnName = "case",
        columnType = TBool
        }
      expectedStream = singleton_stream expectedColumn $ VBool False
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "Case when when else" expectedStream actualStream

test_union :: Assertion
test_union =
  let statement = SQuery $
        CompositeQuery {
           queryCombineOperation = QueryCombineUnion,
           queryConstituentQueries = V.fromList [
             SingleQuery { queryProject = V.singleton $ ELiteral $ LBool True,
                           querySource = Nothing },
             SingleQuery { queryProject = V.singleton $ ELiteral $ LBool False,
                           querySource = Nothing }
           ]
         }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn [
        VBool True,
        VBool False
        ]
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "Union" expectedStream actualStream
        

interpreterTests :: Test.Framework.Test
interpreterTests =
  testGroup "Interpreter" [
    testCase "Boolean select" test_booleanSelect,
    testCase "Rename" test_rename,
    testCase "Case when(value)" test_case_when_value,
    testCase "Case when(null)" test_case_when_null,
    testCase "Case when else" test_case_when_else,
    testCase "Case when when" test_case_when_when,
    testCase "Union" test_union
    ]
