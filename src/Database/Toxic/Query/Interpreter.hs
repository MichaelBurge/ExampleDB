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
  EAggregate aggregate _ -> aggregateType $ aggregateFunctionFromBuiltin aggregate
  EPlaceholder _ -> error "expressionType: Placeholder given"

expressionName :: Expression -> T.Text
expressionName expression = case expression of
  ELiteral _ -> "literal"
  ERename _ x -> x
  ECase _ _ -> "case"
  EVariable name -> name
  EAggregate aggregate _ -> aggregateName $ aggregateFunctionFromBuiltin aggregate
  EPlaceholder x -> T.cons '$' $ T.pack $ show x

expressionColumn :: Expression -> Column
expressionColumn expression = Column {
  columnType = expressionType expression,
  columnName = expressionName expression
  }

-- | Splits a syntax tree on its aggregates, leaving a placeholder variable
getAggregates :: Expression -> (Expression, ArrayOf (QueryAggregate, Expression))
getAggregates expression =
  case expression of
    ELiteral _ -> (expression, V.empty)
    ERename x name -> case getAggregates x of
      (continuation, aggregates) ->
        (ERename continuation name, aggregates)
    ECase _ _ -> error "getAggregates: Aggregates in case expressions unsupported"
    EVariable _ -> (expression, V.empty)
    EAggregate aggregate argument ->
      (EPlaceholder 0, V.singleton (aggregate, argument))
    EPlaceholder x -> (expression, V.empty)

hasAggregates :: Expression -> Bool
hasAggregates expression =
  case expression of
    ECase _ _ -> False
    ELiteral _ -> False
    ERename x _ -> hasAggregates x
    EAggregate _ _ -> True
    EPlaceholder _ -> False
    EVariable _ -> False
  
queryColumns :: Query -> ArrayOf Column
queryColumns query = V.map expressionColumn (queryProject query)

evaluateLiteral :: Literal -> Value
evaluateLiteral literal = case literal of
  LBool x -> VBool x
  LNull -> VNull

-- | Evaluates 
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
  EAggregate _ _ -> error "evaluateOneExpression: Cannot evaluate an aggregate function over a single binding context"

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

-- | Evaluates a query that has no aggregates
evaluateRowWiseQuery :: Environment -> Query -> IO Stream
evaluateRowWiseQuery environment query@(SingleQuery _ _ _) =
  let streamHeader     = queryColumns query
      queryExpressions = queryProject query :: ArrayOf Expression
  in do
    bindingContexts <- resolveQueryBindings environment query
    let streamRecords = map (evaluateExpressions queryExpressions) bindingContexts
    return Stream {
      streamHeader = streamHeader,
      streamRecords = streamRecords
      }

unifyAggregates :: ArrayOf (Expression, ArrayOf(QueryAggregate, Expression)) -> (ArrayOf Expression, ArrayOf AggregateFunction)
unifyAggregates = error "unifyAggregates: Unimplemented"

getQueryPrimaryKey :: Query -> ArrayOf Expression
getQueryPrimaryKey query = error "getQueryPrimaryKey: Unimplemented"

getRowAggregates :: ArrayOf Expression -> (ArrayOf Expression, ArrayOf AggregateFunction)
getRowAggregates expressions =
  let splitExpressions = V.map getAggregates expressions
      (continuations, aggregates) = unifyAggregates splitExpressions
  in (continuations, aggregates)

-- | Evaluates a query with aggregates over a primary key
evaluateAggregateQuery :: Environment -> Query -> IO Stream
evaluateAggregateQuery environment query@(SingleQuery _ _ _) =
  let streamHeader = queryColumns query
      selectExpressions = queryProject query
      groupByExpressions = getQueryPrimaryKey query
  in do
    bindingContexts <- resolveQueryBindings environment query
    let (inputExpressions, aggregates) = getRowAggregates selectExpressions
    let bindAggregateInputs context =
          let primaryKey = evaluateExpressions groupByExpressions context
              aggregateInputs = evaluateExpressions inputExpressions context
          in (primaryKey, aggregateInputs)
    let aggregateInputs = map bindAggregateInputs bindingContexts
    let summarization = summarizeByKey aggregateInputs aggregates
    return $ Stream {
      streamHeader = streamHeader,
      streamRecords = M.elems summarization
      }

evaluateSingleQuery :: Environment -> Query -> IO Stream
evaluateSingleQuery environment query@(SingleQuery _ _ _) =
  if V.any hasAggregates $ queryProject query
  then evaluateAggregateQuery environment query
  else evaluateRowWiseQuery environment query     
  
    
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
    SingleQuery _ _ _ -> evaluateSingleQuery environment query
    SumQuery QuerySumUnionAll queries ->
      evaluateUnionAllQuery environment queries
    ProductQuery queryFactors ->
      evaluateProductQuery environment queryFactors

execute :: Environment -> Statement -> IO Stream
execute environment statement = case statement of
  SQuery query -> evaluateQuery environment query

