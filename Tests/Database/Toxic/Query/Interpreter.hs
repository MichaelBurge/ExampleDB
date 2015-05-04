{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.Query.Interpreter (interpreterTests) where

import Control.Monad.Trans.State
import qualified Data.Map as M
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
  let statement = singleton_query expression
  actualStream <- evaluateQuery nullEnvironment statement
  let expectedColumn = Column {
        columnName = "literal",
        columnType = TBool
        }
  let expectedStream = singleton_stream expectedColumn $ VBool True
  assertEqual "Boolean select" expectedStream actualStream

test_rename :: Assertion
test_rename = do
  let expression = ERename (ELiteral $ LBool True) "example"
  let statement  = singleton_query expression
  actualStream <- evaluateQuery nullEnvironment statement
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
      statement  = singleton_query expression
      expectedColumn = Column {
        columnName = "case",
        columnType = TBool
        }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- evaluateQuery nullEnvironment statement
    assertEqual "Case when(value)" expectedStream actualStream

test_case_when_null :: Assertion
test_case_when_null =
  let condition  = (ELiteral $ LBool False, ELiteral $ LBool True)
      expression = ECase (V.singleton condition) Nothing
      statement  = singleton_query expression
      expectedColumn = Column {
        columnName = "case",
        columnType = TBool
        }
      expectedStream = singleton_stream expectedColumn $ VNull
  in do
    actualStream <- evaluateQuery nullEnvironment statement
    assertEqual "Case when(null)" expectedStream actualStream

test_case_when_else :: Assertion
test_case_when_else =
  let condition  = (ELiteral $ LBool False, ELiteral $ LBool True)
      otherwise  = Just $ ELiteral $ LBool False
      expression = ECase (V.singleton condition) otherwise
      statement  = singleton_query expression
      expectedColumn = Column {
        columnName = "case",
        columnType = TBool
        }
      expectedStream = singleton_stream expectedColumn $ VBool False
  in do
    actualStream <- evaluateQuery nullEnvironment statement
    assertEqual "Case when when" expectedStream actualStream

test_case_when_when :: Assertion
test_case_when_when =
  let condition1 = (ELiteral $ LBool False, ELiteral $ LBool True)
      condition2 =(ELiteral $ LBool True, ELiteral $ LBool False)
      otherwise  = Just $ ELiteral $ LBool True
      expression = ECase (V.fromList [condition1,condition2]) otherwise
      statement  = singleton_query expression
      expectedColumn = Column {
        columnName = "case",
        columnType = TBool
        }
      expectedStream = singleton_stream expectedColumn $ VBool False
  in do
    actualStream <- evaluateQuery nullEnvironment statement
    assertEqual "Case when when else" expectedStream actualStream

test_union :: Assertion
test_union =
  let query =
        SumQuery {
           queryCombineOperation = QuerySumUnionAll,
           queryConstituentQueries = V.fromList [
             singleton_query $ ELiteral $ LBool True,
             singleton_query $ ELiteral $ LBool False
             ]
         }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn [
        VBool True,
        VBool False
        ]
  in do
    actualStream <- evaluateQuery nullEnvironment query
    assertEqual "Union" expectedStream actualStream

test_subquery :: Assertion
test_subquery =
  let query =
        SingleQuery {
          queryProject = V.singleton $ ELiteral $ LBool True,
          querySource = Just $ singleton_query $ ELiteral $ LBool False,
          queryGroupBy = Nothing,
          queryOrderBy = Nothing,
          queryWhere = Nothing
          }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- evaluateQuery nullEnvironment query
    assertEqual "Subquery" expectedStream actualStream

test_cross_join :: Assertion
test_cross_join =
  let query =
        SingleQuery {
          queryGroupBy = Nothing,
          queryOrderBy = Nothing,
          queryWhere = Nothing,
          queryProject = V.singleton $ ELiteral $ LBool True,
          querySource = Just $ ProductQuery {
            queryFactors = V.fromList [
               singleton_query $ ELiteral $ LBool True,
               singleton_query $ ELiteral $ LBool False
               ]
            }
          }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- evaluateQuery nullEnvironment query
    assertEqual "Cross Join" expectedStream actualStream

test_subquery_multiple_rows :: Assertion
test_subquery_multiple_rows =
  let falseQuery = singleton_query $ ELiteral $ LBool False
      unionQuery = SumQuery {
        queryCombineOperation = QuerySumUnionAll,
        queryConstituentQueries = V.fromList [ falseQuery, falseQuery ]
        }
      query = SingleQuery {
        queryProject = V.singleton $ ELiteral $ LBool True,
        querySource = Just unionQuery,
        queryGroupBy = Nothing,
        queryOrderBy = Nothing,
        queryWhere = Nothing
        }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn $ replicate 2 $ VBool True
  in do
    actualStream <- evaluateQuery nullEnvironment query
    assertEqual "" expectedStream actualStream

test_cross_join_multiple_rows :: Assertion
test_cross_join_multiple_rows =
  let falseQuery = singleton_query $ ELiteral $ LBool False
      unionQuery = SumQuery {
        queryCombineOperation = QuerySumUnionAll,
        queryConstituentQueries = V.fromList [ falseQuery, falseQuery ]
        }
      productQuery = ProductQuery {
        queryFactors = V.fromList [ unionQuery, unionQuery, unionQuery ]
        }
      finalQuery = SingleQuery {
        queryProject = V.singleton $ ELiteral $ LBool True,
        querySource = Just productQuery,
        queryGroupBy = Nothing,
        queryOrderBy = Nothing,
        queryWhere = Nothing
        }
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn $ replicate 8 $ VBool True
  in do
    actualStream <- evaluateQuery nullEnvironment finalQuery
    assertEqual "" expectedStream actualStream

test_variable :: Assertion
test_variable =
  let query = SingleQuery {
        queryProject = V.singleton $ EVariable "x",
        querySource = Just $ singleton_query $ ERename (ELiteral $ LBool True) "x",
        queryGroupBy = Nothing,
        queryOrderBy = Nothing,
        queryWhere = Nothing
        }
      expectedColumn = Column { columnName = "x", columnType = TUnknown }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- evaluateQuery nullEnvironment query
    assertEqual "" expectedStream actualStream

test_null :: Assertion
test_null =
  let statement = singleton_query $ ELiteral LNull
      expectedColumn = Column { columnName = "literal", columnType = TUnknown }
      expectedStream = singleton_stream expectedColumn $ VNull
  in do
    actualStream <- evaluateQuery nullEnvironment statement
    assertEqual "" expectedStream actualStream

test_aggregate :: Assertion
test_aggregate =
  let statement = singleton_query $ EAggregate QAggBoolOr $ ELiteral $ LBool True
      expectedColumn = Column { columnName = "bool_or", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in do
    actualStream <- evaluateQuery nullEnvironment statement
    assertEqual "" expectedStream actualStream

test_integer_literal :: Assertion
test_integer_literal =
  let statement = singleton_query $ ELiteral $ LInt 5
      expectedColumn = Column { columnName = "literal", columnType = TInt }
      expectedStream = singleton_stream expectedColumn $ VInt 5
  in do
    actualStream <- evaluateQuery nullEnvironment statement
    assertEqual "" expectedStream actualStream

test_create_table :: Assertion
test_create_table =
  let column =Column { columnName = "example_column", columnType = TBool }
      statement = SCreateTable "example_table" $ TableSpec $ V.singleton column
      expectedTable = Table {
        tableName = "example_table",
        tableSpec = TableSpec $ V.singleton column
        }
      expectedEnvironment = Environment {
        _environmentTables = M.fromList [("example_table", expectedTable)]
        }
  in do
    actualEnvironment <- execStateT (execute statement) nullEnvironment
    assertEqual "" expectedEnvironment actualEnvironment

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
    testCase "Integer literal" test_integer_literal,
    testCase "Create table" test_create_table
    ]
