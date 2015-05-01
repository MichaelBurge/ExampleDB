{-# LANGUAGE OverloadedStrings #-}

module Tests.ExampleQueries (exampleQueriesTests) where

import Database.Toxic.Streams
import Database.Toxic.Types
import Database.Toxic.Query.AST
import Database.Toxic.Query.Interpreter
import Database.Toxic.Query.Parser
import qualified Data.Text as T
import qualified Data.Vector as V

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

assertQueryResults :: Environment -> T.Text -> Stream -> Assertion
assertQueryResults environment query expectedStream = do
  let statement = unsafeRunQueryParser query
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
  let query = "select true union all select false;"
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn [ VBool True, VBool False ]
  in assertQueryResults nullEnvironment query expectedStream

test_subquery :: Assertion
test_subquery =
  let query = "select true from (select false);"
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in assertQueryResults nullEnvironment query expectedStream

test_cross_join :: Assertion
test_cross_join =
  let query = "select true from (select false), (select true);"
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in assertQueryResults nullEnvironment query expectedStream

test_subquery_cross_join_cardinality :: Assertion
test_subquery_cross_join_cardinality =
  let query = "select true from (select false union all select false), (select false union all select false), (select false union all select false);"
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn $ replicate 8 $ VBool True
  in assertQueryResults nullEnvironment query expectedStream

test_null :: Assertion
test_null =
  let query = "select null;"
      expectedColumn = Column { columnName = "literal", columnType = TUnknown }
      expectedStream = singleton_stream expectedColumn VNull
  in assertQueryResults nullEnvironment query expectedStream

test_union_rename :: Assertion
test_union_rename =
  let query = "select true as x union all select false;"
      expectedColumn = Column { columnName = "x", columnType = TBool }
      expectedStream = single_column_stream expectedColumn [ VBool True, VBool False ]
  in assertQueryResults nullEnvironment query expectedStream

test_subquery_union :: Assertion
test_subquery_union =
  let query = "select true from (select false as x union all select true);"
      expectedColumn = Column { columnName = "literal", columnType = TBool }
      expectedStream = single_column_stream expectedColumn [ VBool True, VBool True ]
  in assertQueryResults nullEnvironment query expectedStream

test_aggregate :: Assertion
test_aggregate =
  let query = "select bool_or(x) from (select false as x union all select true);"
      expectedColumn = Column { columnName = "bool_or", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool True
  in assertQueryResults nullEnvironment query expectedStream

test_sum :: Assertion
test_sum =
  let query = "select sum(1) from (select false union all select false);"
      expectedColumn = Column { columnName = "sum", columnType = TInt }
      expectedStream = singleton_stream expectedColumn $ VInt 2
  in assertQueryResults nullEnvironment query expectedStream

test_sum_partitions :: Assertion
test_sum_partitions =
  let query = "select x, sum(1) from (select false as x union all select true union all select true union all select true union all select false union all select null) group by x;"
      expectedHeader = V.fromList [
        Column { columnName = "x", columnType = TUnknown },
        Column { columnName = "sum", columnType = TInt }
        ]
      expectedStream = Stream {
        streamHeader = expectedHeader,
        streamRecords = [
          Record $ V.fromList [ VBool False, VInt 2 ],
          Record $ V.fromList [ VBool True, VInt 3 ],
          Record $ V.fromList [ VNull, VInt 1 ]
          ]
        }
  in assertQueryResults nullEnvironment query expectedStream

test_order_by :: Assertion
test_order_by =
  let query = "select y, x from (select true as y, 5 as x union all select false, 3 union all select true, 3 union all select false, 5) order by x, y descending;"
      expectedHeader = V.fromList [
        Column { columnName = "y", columnType = TUnknown },
        Column { columnName = "x", columnType = TUnknown }
        ]
      expectedStream = Stream {
        streamHeader = expectedHeader,
        streamRecords = map (Record . V.fromList) [
          [ VBool True,  VInt 3 ],
          [ VBool False, VInt 3 ],
          [ VBool True,  VInt 5 ],
          [ VBool False, VInt 5 ]
          ]
        }
  in assertQueryResults nullEnvironment query expectedStream

test_not :: Assertion
test_not =
  let query = "select not true;"
      expectedColumn = Column { columnName = "not", columnType = TBool }
      expectedStream = singleton_stream expectedColumn $ VBool False
  in assertQueryResults nullEnvironment query expectedStream

test_plus :: Assertion
test_plus =
  let query = "select 1 + 1;"
      expectedColumn = Column { columnName = "plus", columnType = TInt }
      expectedStream = singleton_stream expectedColumn $ VInt 2
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
    testCase "Union" test_union,
    testCase "Subquery" test_subquery,
    testCase "Cross Join" test_cross_join,
    testCase "Subquery/cross-join cardinality" test_subquery_cross_join_cardinality,
    testCase "Null" test_null,
    testCase "Union rename" test_union_rename,
    testCase "Aggregate" test_aggregate,
    testCase "Subquery Union" test_subquery_union,
    testCase "Sum" test_sum,
    testCase "Sum partitions" test_sum_partitions,
    testCase "Order by" test_order_by,
    testCase "Not" test_not,
    testCase "Plus" test_plus
    ]
