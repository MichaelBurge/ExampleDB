{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.Query.Parser (parserTests) where

import Database.Toxic.Types
import Database.Toxic.Query.AST
import Database.Toxic.Query.Parser
import Database.Toxic.Query.Tokenizer

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

import qualified Data.Vector as V

assert_tokens_parse :: [ Token ] -> Statement -> Assertion
assert_tokens_parse tokens expectedStatement = do
  let actualStatement   = unsafeRunTokenParser tokens  
  assertEqual "" expectedStatement actualStatement
  
test_booleanSelect :: Assertion
test_booleanSelect =
  let tokens            = [ TkSelect, TkTrue, TkStatementEnd ]
      literalTrue       = ELiteral $ LBool True
      expectedStatement = singleton_statement literalTrue
  in assert_tokens_parse tokens expectedStatement

test_rename :: Assertion
test_rename =
  let tokens =
        [ TkSelect, TkTrue, TkRename, TkIdentifier "example", TkStatementEnd ]
      expectedExpression = ERename (ELiteral $ LBool True) "example"
      expectedStatement = singleton_statement expectedExpression
  in assert_tokens_parse tokens expectedStatement

test_case_when :: Assertion
test_case_when =
  let tokens =
        [ TkSelect, TkCase, TkWhen, TkTrue, TkThen, TkFalse, TkEnd, TkStatementEnd ]
      expectedCondition  = ELiteral $ LBool True
      expectedResult     = ELiteral $ LBool False
      expectedCase       = (expectedCondition, expectedResult)
      expectedExpression = ECase (V.singleton expectedCase) Nothing
      expectedStatement  = singleton_statement expectedExpression
  in assert_tokens_parse tokens expectedStatement

test_case_when_else :: Assertion
test_case_when_else =
  let tokens =
        [
          TkSelect,
            TkCase,
              TkWhen, TkTrue, TkThen, TkTrue,
              TkElse, TkFalse,
            TkEnd,
            TkStatementEnd
        ]
      expectedCondition  = (ELiteral $ LBool True, ELiteral $ LBool True)
      expectedOtherwise  = Just $ ELiteral $ LBool False
      expectedExpression = ECase (V.singleton expectedCondition) expectedOtherwise
      expectedStatement  = singleton_statement expectedExpression
  in assert_tokens_parse tokens expectedStatement

test_case_when_when_else :: Assertion
test_case_when_when_else =
  let tokens =
        [
          TkSelect,
            TkCase,
              TkWhen, TkTrue, TkThen, TkTrue,
              TkWhen, TkFalse, TkThen, TkTrue,
              TkElse, TkFalse,
            TkEnd,
            TkStatementEnd
        ]
      expectedCondition1 = (ELiteral $ LBool True, ELiteral $ LBool True)
      expectedCondition2 = (ELiteral $ LBool False, ELiteral $ LBool True)
      expectedOtherwise  = Just $ ELiteral $ LBool False
      expectedConditions = V.fromList [ expectedCondition1, expectedCondition2 ]
      expectedExpression = ECase expectedConditions expectedOtherwise
      expectedStatement  = singleton_statement expectedExpression
  in assert_tokens_parse tokens expectedStatement

test_union :: Assertion
test_union =
  let tokens =
        [
          TkSelect, TkTrue,
          TkUnion, TkAll,
          TkSelect, TkFalse,
          TkStatementEnd
        ]
      expectedStatement = SQuery $
        SumQuery {
           queryCombineOperation = QuerySumUnionAll,
           queryConstituentQueries = V.fromList [
             SingleQuery { queryProject = V.singleton $ ELiteral $ LBool True,
                           querySource = Nothing },
             SingleQuery { queryProject = V.singleton $ ELiteral $ LBool False,
                           querySource = Nothing }
           ]
         }
  in assert_tokens_parse tokens expectedStatement

test_subquery :: Assertion
test_subquery =
  let tokens =
        [
          TkSelect,
            TkTrue,
          TkFrom, TkOpen,
            TkSelect,
              TkFalse,
          TkClose,
          TkStatementEnd
        ]
      expectedStatement = SQuery $
        SingleQuery {
          queryProject = V.singleton $ ELiteral $ LBool True,
          querySource = Just $ SingleQuery {
            queryProject = V.singleton $ ELiteral $ LBool False,
            querySource = Nothing
            }
          }
  in assert_tokens_parse tokens expectedStatement

test_cross_join :: Assertion
test_cross_join =
  let tokens =
        [
          TkSelect, TkTrue, TkFrom,
          TkOpen, TkSelect, TkTrue, TkClose, TkSequence,
          TkOpen, TkSelect, TkFalse, TkClose,
          TkStatementEnd
        ]
      expectedStatement = SQuery $
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
  in assert_tokens_parse tokens expectedStatement

test_variable :: Assertion
test_variable =
  let tokens =
        [
          TkSelect, TkIdentifier "x", TkFrom,
          TkOpen, TkSelect, TkTrue, TkRename, TkIdentifier "x", TkClose,
          TkStatementEnd
        ]
      expectedStatement = SQuery $
        SingleQuery {
          queryProject = V.singleton $ EVariable "x",
          querySource = Just $ SingleQuery {
            queryProject = V.singleton $ ERename (ELiteral $ LBool True) "x",
            querySource = Nothing
            }
          }
  in assert_tokens_parse tokens expectedStatement

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
    testCase "Variable" test_variable
    ]
  
