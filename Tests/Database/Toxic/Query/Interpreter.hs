{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.Query.Interpreter (interpreterTests) where

import qualified Data.Vector as V

import Database.Toxic.Streams
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
        SumQuery {
           queryCombineOperation = QuerySumUnionAll,
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

test_subquery :: Assertion
test_subquery =
  let statement = SQuery $
        SingleQuery {
          queryProject = V.singleton $ ELiteral $ LBool True,
          querySource = Just $ SingleQuery {
            queryProject = V.singleton $ ELiteral $ LBool False,
            querySource = Nothing
            }
          }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "Subquery" expectedStream actualStream

test_cross_join :: Assertion
test_cross_join =
  let statement = SQuery $
        SingleQuery {
          queryProject = V.singleton $ ELiteral $ LBool True,
          querySource = Just $ ProductQuery {
            queryFactors = V.fromList [
               SingleQuery {
                  queryProject = V.singleton $ ELiteral $ LBool True,
                  querySource = Nothing
                  },
               SingleQuery {
                 queryProject = V.singleton $ ELiteral $ LBool False,
                 querySource = Nothing
                 }
               ]
            }
          }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "Cross Join" expectedStream actualStream

test_subquery_multiple_rows :: Assertion
test_subquery_multiple_rows =
  let falseQuery = SingleQuery {
        queryProject = V.singleton $ ELiteral $ LBool False,
        querySource = Nothing
        }
      unionQuery = SumQuery {
        queryCombineOperation = QuerySumUnionAll,
        queryConstituentQueries = V.fromList [ falseQuery, falseQuery ]
        }
      statement = SQuery $ SingleQuery {
        queryProject = V.singleton $ ELiteral $ LBool True,
        querySource = Just unionQuery
        }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn $ replicate 2 $ VBool True
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "" expectedStream actualStream

test_cross_join_multiple_rows :: Assertion
test_cross_join_multiple_rows =
  let falseQuery = SingleQuery {
        queryProject = V.singleton $ ELiteral $ LBool False,
        querySource = Nothing
        }
      unionQuery = SumQuery {
        queryCombineOperation = QuerySumUnionAll,
        queryConstituentQueries = V.fromList [ falseQuery, falseQuery ]
        }
      productQuery = ProductQuery {
        queryFactors = V.fromList [ unionQuery, unionQuery, unionQuery ]
        }
      finalQuery = SingleQuery {
        queryProject = V.singleton $ ELiteral $ LBool True,
        querySource = Just productQuery
        }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn $ replicate 8 $ VBool True
      statement = SQuery finalQuery
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "" expectedStream actualStream

test_variable :: Assertion
test_variable =
  let query = SingleQuery {
        queryProject = V.singleton $ EVariable "x",
        querySource = Just $ SingleQuery {
          queryProject = V.singleton $ ERename (ELiteral $ LBool True) "x",
          querySource = Nothing
          }
        }
      expectedColumn = Column { columnName = "x", columnType = TUnknown }
      expectedStream = singleton_stream expectedColumn $ VBool True
      statement = SQuery query
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "" expectedStream actualStream

test_null :: Assertion
test_null =
  let statement = singleton_statement $ ELiteral LNull
      expectedColumn = Column { columnName = "literal", columnType = TUnknown }
      expectedStream = singleton_stream expectedColumn $ VNull
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "" expectedStream actualStream

test_aggregate :: Assertion
test_aggregate =
  let statement = singleton_statement $ EAggregate QAggBoolOr $ ELiteral $ LBool True
      expectedColumn = Column { columnName = "bool_or", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "" expectedStream actualStream

test_integer_literal :: Assertion
test_integer_literal =
  let statement = singleton_statement $ ELiteral $ LInt 5
      expectedColumn = Column { columnName = "literal", columnType = TInt }
      expectedStream = singleton_stream expectedColumn $ VInt 5
  in do
    actualStream <- execute nullEnvironment statement
    assertEqual "" expectedStream actualStream

interpreterTests :: Test.Framework.Test
interpreterTests =
  testGroup "Interpreter" [
    testCase "Boolean select" test_booleanSelect,
    testCase "Rename" test_rename,
    testCase "Case when(value)" test_case_when_value,
    testCase "Case when(null)" test_case_when_null,
    testCase "Case when else" test_case_when_else,
    testCase "Case when when" test_case_when_when,
    testCase "Union" test_union,
    testCase "Subquery" test_subquery,
    testCase "Subquery(Multiple rows" test_subquery_multiple_rows,
    testCase "Cross Join" test_cross_join,
    testCase "Variable" test_variable,
    testCase "Null" test_null,
    testCase "Aggregate" test_aggregate,
    testCase "Integer literal" test_integer_literal
    ]
