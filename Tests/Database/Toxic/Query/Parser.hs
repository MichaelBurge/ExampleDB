{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.Query.Parser (parserTests) where

import Database.Toxic.Types
import Database.Toxic.Query.AST
import Database.Toxic.Query.Parser

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

import qualified Data.Text as T
import qualified Data.Vector as V

assertQueryParses :: T.Text -> Statement -> Assertion
assertQueryParses text expectedStatement =
  let actualStatement = unsafeRunQueryParser text
  in assertEqual "" expectedStatement actualStatement
  
test_booleanSelect :: Assertion
test_booleanSelect =
  let query             = "select true;"
      literalTrue       = ELiteral $ LBool True
      expectedStatement = singleton_statement literalTrue
  in assertQueryParses query expectedStatement

test_rename :: Assertion
test_rename =
  let query = "select true as example;"
      expectedExpression = ERename (ELiteral $ LBool True) "example"
      expectedStatement = singleton_statement expectedExpression
  in assertQueryParses query expectedStatement

test_case_when :: Assertion
test_case_when =
  let query = "select case when true then false end;"
      expectedCondition  = ELiteral $ LBool True
      expectedResult     = ELiteral $ LBool False
      expectedCase       = (expectedCondition, expectedResult)
      expectedExpression = ECase (V.singleton expectedCase) Nothing
      expectedStatement  = singleton_statement expectedExpression
  in assertQueryParses query expectedStatement

test_case_when_else :: Assertion
test_case_when_else =
  let query = "select case when true then true else false end;"
      expectedCondition  = (ELiteral $ LBool True, ELiteral $ LBool True)
      expectedOtherwise  = Just $ ELiteral $ LBool False
      expectedExpression = ECase (V.singleton expectedCondition) expectedOtherwise
      expectedStatement  = singleton_statement expectedExpression
  in assertQueryParses query expectedStatement

test_case_when_when_else :: Assertion
test_case_when_when_else =
  let query = "select case when true then true when false then true else false end;"
      expectedCondition1 = (ELiteral $ LBool True, ELiteral $ LBool True)
      expectedCondition2 = (ELiteral $ LBool False, ELiteral $ LBool True)
      expectedOtherwise  = Just $ ELiteral $ LBool False
      expectedConditions = V.fromList [ expectedCondition1, expectedCondition2 ]
      expectedExpression = ECase expectedConditions expectedOtherwise
      expectedStatement  = singleton_statement expectedExpression
  in assertQueryParses query expectedStatement

test_union :: Assertion
test_union =
  let query = "select true union all select false;"
      expectedStatement = SQuery $
        SumQuery {
           queryCombineOperation = QuerySumUnionAll,
           queryConstituentQueries = V.fromList [
             singleton_query $ ELiteral $ LBool True,
             singleton_query $ ELiteral $ LBool False
           ]
         }
  in assertQueryParses query expectedStatement

test_subquery :: Assertion
test_subquery =
  let query = "select true from ( select false );"
      expectedStatement = SQuery $
        SingleQuery {
          queryGroupBy = Nothing,
          queryOrderBy = Nothing,
          queryProject = V.singleton $ ELiteral $ LBool True,
          querySource = Just $ singleton_query $ ELiteral $ LBool False,
          queryWhere = Nothing
          }
  in assertQueryParses query expectedStatement

test_cross_join :: Assertion
test_cross_join =
  let query = "select true from (select true), (select false);"
      expectedStatement = SQuery $
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
  in assertQueryParses query expectedStatement

test_variable :: Assertion
test_variable =
  let query = "select x from (select true as x);"
      expectedStatement = SQuery $
        SingleQuery {
          queryGroupBy = Nothing,
          queryOrderBy = Nothing,
          queryWhere = Nothing,
          queryProject = V.singleton $ EVariable "x",
          querySource = Just $ singleton_query $ ERename (ELiteral $ LBool True) "x"
          }
  in assertQueryParses query expectedStatement

test_null :: Assertion
test_null =
  let query = "select null;"
      expectedStatement = singleton_statement $ ELiteral LNull
  in assertQueryParses query expectedStatement

test_bool_or :: Assertion
test_bool_or =
  let query = "select bool_or(true);"
      expectedStatement = singleton_statement $ EAggregate QAggBoolOr $ ELiteral $ LBool True
  in assertQueryParses query expectedStatement

test_group_by :: Assertion
test_group_by =
  let query = "select true group by true;"
      expectedStatement = SQuery SingleQuery {
        queryProject = V.singleton $ ELiteral $ LBool True,
        queryGroupBy = Just $ V.singleton $ ELiteral $ LBool True,
        querySource = Nothing,
        queryOrderBy = Nothing,
        queryWhere = Nothing
        }
  in assertQueryParses query expectedStatement

test_aggregate_from :: Assertion
test_aggregate_from =
  let query = "select bool_or(x) from (select false as x union all select true);"
      expectedStatement = SQuery SingleQuery {
        queryProject = V.singleton $ EAggregate QAggBoolOr $ EVariable "x",
        queryGroupBy = Nothing,
        queryOrderBy = Nothing,
        queryWhere = Nothing,
        querySource = Just $ SumQuery {
          queryCombineOperation = QuerySumUnionAll,
          queryConstituentQueries = V.fromList [
            singleton_query $ ERename (ELiteral $ LBool False) "x",
            singleton_query $ ELiteral $ LBool True
            ]
          }
        }
  in assertQueryParses query expectedStatement

test_integer_literal :: Assertion
test_integer_literal =
  let query = "select 5;"
      expectedStatement = singleton_statement $ ELiteral $ LInt 5
  in assertQueryParses query expectedStatement

test_order_by :: Assertion
test_order_by =
  let query = "select true order by true;"
      expectedStatement = SQuery SingleQuery {
        queryProject = V.singleton $ ELiteral $ LBool True,
        queryGroupBy = Nothing,
        querySource = Nothing,
        queryWhere = Nothing,
        queryOrderBy = Just $ V.singleton (ELiteral $ LBool True, Ascending)
        }
  in assertQueryParses query expectedStatement

test_multiple_fields :: Assertion
test_multiple_fields =
  let query = "select 5,5;"
      expectedStatement = SQuery SingleQuery {
        queryProject = V.fromList [ ELiteral $ LInt 5, ELiteral $ LInt 5 ],
        queryGroupBy = Nothing,
        querySource = Nothing,
        queryOrderBy = Nothing,
        queryWhere = Nothing
        }
  in assertQueryParses query expectedStatement

test_create_table :: Assertion
test_create_table =
  let query = "create table derp (num_derps int);"
      expectedStatement = SCreateTable "derp" $ TableSpec $ V.singleton
        Column { columnName = "num_derps", columnType = TInt }
  in assertQueryParses query expectedStatement

test_where :: Assertion
test_where =
  let query = "select x from (select 5 as x union all select 10) where x < 7;"
      expectedStatement = SQuery SingleQuery {
        queryProject = V.singleton $ EVariable "x",
        queryGroupBy = Nothing,
        queryOrderBy = Nothing,
        queryWhere = Just $ EBinop BinopLess (EVariable "x") (ELiteral $ LInt 7),
        querySource = Just SumQuery {
          queryCombineOperation = QuerySumUnionAll,
          queryConstituentQueries = V.fromList [
            singleton_query $ ERename (ELiteral $ LInt 5) "x",
            singleton_query $ ELiteral $ LInt 10
            ]
          }
        }
  in assertQueryParses query expectedStatement

parserTests :: Test.Framework.Test
parserTests =
  testGroup "Parser" [
    testCase "Boolean select" test_booleanSelect,
    testCase "Rename" test_rename,
    testCase "Case When" test_case_when,
    testCase "Case when else" test_case_when_else,
    testCase "Case when when else" test_case_when_when_else,
    testCase "Union" test_union,
    testCase "Subquery" test_subquery,
    testCase "Cross Join" test_cross_join,
    testCase "Variable" test_variable,
    testCase "Null" test_null,
    testCase "bool_or()" test_bool_or,
    testCase "Group by" test_group_by,
    testCase "Aggregate from" test_aggregate_from,
    testCase "Integer literal" test_integer_literal,
    testCase "Order by" test_order_by,
    testCase "Multiple fields" test_multiple_fields,
    testCase "Create Table" test_create_table,
    testCase "Where" test_where
    ]
  
