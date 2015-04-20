{-# LANGUAGE OverloadedStrings,RankNTypes #-}

module Database.Toxic.Query.Interpreter where

import Database.Toxic.Aggregates
import Database.Toxic.Query.AST
import Database.Toxic.Streams
import Database.Toxic.Types

import Control.Applicative
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V

data Environment = Environment { }

newtype BindingContext = BindingContext (M.Map T.Text Value)

nullEnvironment :: Environment
nullEnvironment = error "TODO: Implement nullEnvironment"

nullContext :: BindingContext
nullContext = error "TODO: Implement nullContext"

literalType :: Literal -> Type
literalType literal = case literal of
  LBool _ -> TBool
  LNull -> TUnknown

lookup_value :: BindingContext -> T.Text -> Value
lookup_value (BindingContext context) name =
  case M.lookup name context of
    Just value -> value
    Nothing -> error $ "Unknown variable " ++ show name

aggregateFunctionFromBuiltin :: QueryAggregate -> AggregateFunction
aggregateFunctionFromBuiltin aggregate =
  case aggregate of
    QAggBoolOr -> bool_or

expressionType :: Expression -> Type
expressionType expression = case expression of
  ELiteral x -> literalType x
  ERename x _ -> expressionType x
  ECase vs _ -> expressionType $ snd $ V.head vs
  EVariable name -> TUnknown
  EAggregate aggregate -> aggregateType $ aggregateFunctionFromBuiltin aggregate

expressionName :: Expression -> T.Text
expressionName expression = case expression of
  ELiteral _ -> "literal"
  ERename _ x -> x
  ECase _ _ -> "case"
  EVariable name -> name
  EAggregate aggregate -> aggregateName $ aggregateFunctionFromBuiltin aggregate

expressionColumn :: Expression -> Column
expressionColumn expression = Column {
  columnType = expressionType expression,
  columnName = expressionName expression
  }

queryColumns :: Query -> ArrayOf Column
queryColumns query = V.map expressionColumn (queryProject query)

evaluateLiteral :: Literal -> Value
evaluateLiteral literal = case literal of
  LBool x -> VBool x
  LNull -> VNull

-- TODO: This is only for row-wise expressions, not aggregates.
evaluateOneExpression :: BindingContext -> Expression -> Value
evaluateOneExpression context expression = case expression of
  ELiteral literal -> evaluateLiteral literal
  ERename x _ -> evaluateOneExpression context x
  ECase conditions otherwise ->
    let isTrue (condition, result) =
          evaluateOneExpression context condition == VBool True
        firstMatchingCondition = V.find isTrue conditions
    in case firstMatchingCondition of
      Just (_, result) -> evaluateOneExpression context result
      Nothing -> case otherwise of
        Just x -> evaluateOneExpression context x
        Nothing -> VNull
  EVariable name -> lookup_value context name
  EAggregate _ -> error "evaluateOneExpression: Cannot evaluate an aggregate function over a single binding context"

evaluateExpressions :: ArrayOf Expression -> BindingContext -> Record
evaluateExpressions expressions context = 
  Record $ V.map (evaluateOneExpression context) expressions

transformStreamToBindings :: Stream -> SetOf BindingContext
transformStreamToBindings stream =
  let unwrapRecord (Record values) = values
      names = V.map columnName $ streamHeader stream
      nameValuePairs = map (V.zip names) $
                       map unwrapRecord $
                       streamRecords stream :: [ArrayOf (T.Text, Value) ]
      bindingContexts = map (BindingContext . M.fromList . V.toList) nameValuePairs
  in bindingContexts

resolveQueryBindings :: Environment -> Query -> IO (SetOf BindingContext)
resolveQueryBindings environment query =
  let mapResults innerQuery = do
        innerStream <- evaluateQuery environment innerQuery
        return $ transformStreamToBindings innerStream
  in case query of
    SingleQuery { querySource = Nothing } -> return [nullContext]
    SingleQuery { querySource = Just innerQuery } -> mapResults innerQuery
    SumQuery { } -> mapResults query
    ProductQuery { } -> mapResults query
    
evaluateSingleQuery :: Environment -> Query -> IO Stream
evaluateSingleQuery environment query@(SingleQuery _ _) =
  let streamHeader     = queryColumns query
      queryExpressions = queryProject query :: ArrayOf Expression
  in do
    bindingContexts <- resolveQueryBindings environment query
    let streamRecords = map (evaluateExpressions queryExpressions) bindingContexts
    return Stream {
      streamHeader = streamHeader,
      streamRecords = streamRecords
      }
    
evaluateSingleQuery _ query = error $ "evaluateSingleQuery called on something other than a single query" ++ show query

evaluateUnionAllQuery :: Environment -> ArrayOf Query -> IO Stream
evaluateUnionAllQuery environment queries =
  let firstQuery = V.head queries
      header     = queryColumns firstQuery
      combinedRecords = concatMapM (\x -> streamRecords <$> evaluateQuery environment x) (V.toList queries)
  in do
    records <- combinedRecords
    return $ Stream {
      streamHeader = header,
      streamRecords = records
    }

concatMapM f l = fmap concat (mapM f l)

evaluateProductQuery :: Environment -> ArrayOf Query -> IO Stream
evaluateProductQuery environment queries =
  let header = V.empty
      evaluateOneStream query = streamRecords <$> evaluateQuery environment query :: IO (SetOf Record)
      productRecords = map (const $ Record V.empty) <$> (sequence <$> (mapM evaluateOneStream $ V.toList queries)) :: IO [Record]
  in do
    records <- productRecords
    return $ Stream {
      streamHeader = header,
      streamRecords = records
    }

evaluateQuery :: Environment -> Query -> IO Stream
evaluateQuery environment query =
  case query of
    SingleQuery _ _ -> evaluateSingleQuery environment query
    SumQuery QuerySumUnionAll queries ->
      evaluateUnionAllQuery environment queries
    ProductQuery queryFactors ->
      evaluateProductQuery environment queryFactors

execute :: Environment -> Statement -> IO Stream
execute environment statement = case statement of
  SQuery query -> evaluateQuery environment query

