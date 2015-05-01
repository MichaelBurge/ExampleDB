{-# LANGUAGE OverloadedStrings,RankNTypes #-}

module Database.Toxic.Query.Interpreter where

import qualified Database.Toxic.Aggregates as Agg
import Database.Toxic.Operators
import Database.Toxic.Query.AST
import Database.Toxic.Streams
import Database.Toxic.Types

import Control.Applicative
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as V

data Environment = Environment { }

data BindingContext = BindingContext {
  bindingVariables    :: M.Map T.Text Value,
  bindingPlaceholders :: ArrayOf Value
  } deriving (Eq, Show)

nullEnvironment :: Environment
nullEnvironment = error "TODO: Implement nullEnvironment"

nullContext :: BindingContext
nullContext = BindingContext {
  bindingVariables = M.empty,
  bindingPlaceholders = V.empty
  }

literalType :: Literal -> Type
literalType literal = case literal of
  LBool _ -> TBool
  LInt _ -> TInt
  LNull -> TUnknown
  LValue x -> valueType x

lookupVariable :: BindingContext -> T.Text -> Value
lookupVariable context name =
  case M.lookup name $ bindingVariables context of
    Just value -> value
    Nothing -> error $ "Unknown variable " ++ show name

lookupPlaceholder :: BindingContext -> Int -> Value
lookupPlaceholder context index = (V.!) (bindingPlaceholders context) index

variablesToContext :: M.Map T.Text Value -> BindingContext
variablesToContext bindings = BindingContext {
  bindingVariables = bindings,
  bindingPlaceholders = V.empty
  }

placeholdersToContext :: ArrayOf Value -> BindingContext
placeholdersToContext bindings = BindingContext {
  bindingVariables = M.empty,
  bindingPlaceholders = bindings
  }

aggregateFunctionFromBuiltin :: QueryAggregate -> AggregateFunction
aggregateFunctionFromBuiltin aggregate =
  case aggregate of
    QAggBoolOr -> Agg.bool_or
    QAggFailIfAggregated -> Agg.fail_if_aggregated
    QAggSum -> Agg.sum



unopName :: Unop -> T.Text
unopName UnopNot = "not"

unopType :: Unop -> Type
unopType UnopNot = TBool

binopType :: Binop -> Type
binopType BinopPlus = TInt
binopType BinopMinus = TInt
binopType BinopTimes = TInt
binopType BinopDividedBy = TInt
binopType BinopGreaterOrEqual = TBool
binopType BinopGreater = TBool
binopType BinopLess = TBool
binopType BinopLessOrEqual = TBool
binopType BinopEqual = TBool
binopType BinopUnequal = TBool

binopName :: Binop -> T.Text
binopName BinopPlus = "plus"
binopName BinopMinus = "minus"
binopName BinopTimes = "times"
binopName BinopDividedBy = "div"
binopName BinopGreaterOrEqual = "greaterOrEqual"
binopName BinopGreater = "greater"
binopName BinopLess = "less"
binopName BinopLessOrEqual = "lessOrEqual"
binopName BinopEqual = "equal"
binopName BinopUnequal = "unequal"

expressionType :: Expression -> Type
expressionType expression = case expression of
  EUnop unop _ -> unopType unop
  EBinop binop _ _ -> binopType binop
  ELiteral x -> literalType x
  ERename x _ -> expressionType x
  ECase vs _ -> expressionType $ snd $ V.head vs
  EVariable name -> TUnknown
  EAggregate aggregate _ -> aggregateType $ aggregateFunctionFromBuiltin aggregate
  EPlaceholder _ -> error "expressionType: Placeholder given"

expressionName :: Expression -> T.Text
expressionName expression = case expression of
  EUnop unop _ -> unopName unop
  EBinop binop _ _ -> binopName binop
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
-- | Yields: An input transformer, an aggregate, and an output transformer
splitAggregate :: Expression -> Maybe (Expression, QueryAggregate, Expression)
splitAggregate expression =
  let identity = EPlaceholder 0
  in case expression of
    ELiteral _ -> Nothing
    ERename x name -> case splitAggregate x of
      Just (continuation, aggregate, transform) ->
        Just $ (ERename continuation name, aggregate, transform)
      Nothing -> Nothing
    ECase _ _ -> error "splitAggregate: Aggregates in case expressions unsupported"
    EVariable _ -> Nothing
    EAggregate aggregate argument ->
      Just $ (identity, aggregate, argument)
    EPlaceholder x -> Nothing
    EUnop unop x -> case splitAggregate x of
      Just (continuation, aggregate, transform) ->
        Just $ (EUnop unop continuation, aggregate, transform)
      Nothing -> Nothing
    EBinop binop x y -> case (splitAggregate x, splitAggregate y) of
      (Nothing, Nothing) -> Nothing
      (Just (c1, a1, t1), Nothing) -> Just (EBinop binop c1 y, a1, t1)
      (Nothing, Just (c1, a1, t1)) -> Just (EBinop binop x c1, a1, t1)
      (Just (c1, a1, t1), Just (c2, a2, t2)) ->
        error "splitAggregate: Aggregates on both sides of a binary operator are unsupported"

-- | Same as splitAggregate, but replaces non-aggregate expressions with a placeholder
-- | that fails if aggregated.
getAggregate :: Expression -> (Expression, QueryAggregate, Expression)
getAggregate expression = case splitAggregate expression of
  Just x -> x
  Nothing -> (EPlaceholder 0, QAggFailIfAggregated, expression)

hasAggregates :: Expression -> Bool
hasAggregates expression =
  case expression of
    ECase _ _ -> False
    ELiteral _ -> False
    EUnop _ x -> hasAggregates x
    EBinop _ x y -> hasAggregates x || hasAggregates y
    ERename x _ -> hasAggregates x
    EAggregate _ _ -> True
    EPlaceholder _ -> False
    EVariable _ -> False
  
queryColumns :: Query -> ArrayOf Column
queryColumns query = V.map expressionColumn (queryProject query)

evaluateLiteral :: Literal -> Value
evaluateLiteral literal = case literal of
  LBool x -> VBool x
  LInt x -> VInt $ fromInteger x
  LNull -> VNull
  LValue x -> x

applyUnop :: Unop -> Value -> Value
applyUnop UnopNot = operatorNot

applyBinop :: Binop -> Value -> Value -> Value
applyBinop BinopPlus = operatorPlus
applyBinop BinopMinus = operatorMinus
applyBinop BinopTimes = operatorTimes
applyBinop BinopDividedBy = operatorDividedBy
applyBinop BinopGreaterOrEqual = operatorGreaterOrEqual
applyBinop BinopGreater = operatorGreater
applyBinop BinopLess = operatorLess
applyBinop BinopLessOrEqual = operatorLessOrEqual
applyBinop BinopEqual = operatorEqual
applyBinop BinopUnequal = operatorUnequal


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
  EVariable name -> lookupVariable context name
  EAggregate queryAggregate inner ->
    let aggregate = aggregateFunctionFromBuiltin queryAggregate
        innerResult = evaluateOneExpression context inner
    in aggregateFinalize aggregate $
       aggregateAccumulate aggregate innerResult $
       aggregateInitialize aggregate
  EPlaceholder n -> lookupPlaceholder context n
  EUnop unop inner -> applyUnop unop $ evaluateOneExpression context inner
  EBinop binop x1 x2 ->
    let v1 = evaluateOneExpression context x1
        v2 = evaluateOneExpression context x2
    in applyBinop binop v1 v2
  

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
      bindingContexts = map (variablesToContext . M.fromList . V.toList) nameValuePairs
  in bindingContexts

-- | Describes the bindings that are available when evaluating a query, not the
-- | bindings that result from the query
resolveQueryBindings :: Environment -> Query -> IO (SetOf BindingContext)
resolveQueryBindings environment query =
  let mapResults innerQuery = do
        innerStream <- evaluateQuery environment innerQuery
        return $ transformStreamToBindings innerStream
  in case query of
    SingleQuery { querySource = Nothing } -> return [ nullContext ]

    SingleQuery { querySource = Just innerQuery } -> mapResults innerQuery
    SumQuery { } -> mapResults query
    ProductQuery { } -> mapResults query

-- | Evaluates a query that has no aggregates
evaluateRowWiseQuery :: Environment -> Query -> IO Stream
evaluateRowWiseQuery environment query@(SingleQuery _ _ _ _) =
  let streamHeader     = queryColumns query
      queryExpressions = queryProject query :: ArrayOf Expression
  in do
    bindingContexts <- resolveQueryBindings environment query
    let streamRecords = map (evaluateExpressions queryExpressions) bindingContexts
    return $ Stream {
      streamHeader = streamHeader,
      streamRecords = streamRecords
      }

getQueryPrimaryKey :: Query -> ArrayOf Expression
getQueryPrimaryKey query = case queryGroupBy query of
  Just x -> x
  Nothing -> V.singleton $ ELiteral $ LBool True

getRowAggregates :: ArrayOf Expression -> (ArrayOf Expression, ArrayOf QueryAggregate, ArrayOf Expression)
getRowAggregates expressions =
  let splits = V.map getAggregate expressions
      continuations     = V.map (\(x,_,_) -> x) splits
      aggregates        = V.map (\(_,x,_) -> x) splits
      inputTransformers = V.map (\(_,_,x) -> x) splits
  in (continuations, aggregates, inputTransformers)

getColumnOrdering :: Query -> Maybe (ArrayOf (Int, StreamOrder))
getColumnOrdering query =
  case queryOrderBy query of
    Just orderBys ->
      let findExpression :: Expression -> Int
          findExpression expression =
            let selects = queryProject query
            in case V.findIndex (== expression) selects of
              Just idx -> idx
              Nothing -> error $ "getColumnOrdering: Unable to find an order by entry in the select clause"
       in Just $
          V.map (\(expr, order) -> (findExpression expr, order)) $
          orderBys
    Nothing -> Nothing

-- | Always a replacement function on each subtree of the original expression
mapAst :: Expression -> (Expression -> Expression) -> Expression
mapAst expression f =
  let mapChildren :: Expression -> Expression
      mapChildren parent = case expression of
        ELiteral _ -> parent
        ERename child name -> ERename (mapChildren child) name
        ECase _ _ -> error "mapAst: Unsupported for case expressions"
        EVariable name -> parent
        EAggregate aggregate child -> EAggregate aggregate $ f child
        EPlaceholder _ -> parent
        EUnop unop x -> EUnop unop (mapChildren x)
        EBinop binop x y -> EBinop binop (mapChildren x) (mapChildren y)
  in f $ mapChildren expression

-- | Evaluates a query with aggregates over a primary key
evaluateAggregateQuery :: Environment -> Query -> IO Stream
evaluateAggregateQuery environment query@(SingleQuery _ _ _ _) =
  let streamHeader = queryColumns query
      selectExpressions = queryProject query
      groupByExpressions = getQueryPrimaryKey query
  in do
    bindingContexts <- resolveQueryBindings environment query
    let (continuations, aggregates, inputTransformers) = getRowAggregates selectExpressions
    let aggregateFunctions = V.map aggregateFunctionFromBuiltin aggregates
    let bindAggregateInputs context =
          let primaryKey = evaluateExpressions groupByExpressions context
              aggregateInputs = evaluateExpressions inputTransformers context
          in (primaryKey, aggregateInputs)
    let aggregateInputs = map bindAggregateInputs bindingContexts
    let summarization = summarizeByKey aggregateInputs aggregateFunctions
        finalizeValue :: Expression -> Value -> Value
        finalizeValue expression value =
          let context = placeholdersToContext $ V.singleton value
          in evaluateOneExpression context expression
        finalized :: M.Map PrimaryKey Record
        finalized = M.map (\(Record vs) -> Record $ V.zipWith finalizeValue continuations vs) summarization
    return $ Stream {
      streamHeader = streamHeader,
      streamRecords = M.elems finalized
      }

evaluateSingleQuery :: Environment -> Query -> IO Stream
evaluateSingleQuery environment query@(SingleQuery _ _ _ _) = do
  unorderedStream <-
    if V.any hasAggregates $ queryProject query
    then evaluateAggregateQuery environment query
    else evaluateRowWiseQuery environment query
  return $ case getColumnOrdering query of
    Just orderings -> orderByColumns unorderedStream orderings
    Nothing -> unorderedStream
  
    
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
    SingleQuery _ _ _ _ -> evaluateSingleQuery environment query
    SumQuery QuerySumUnionAll queries ->
      evaluateUnionAllQuery environment queries
    ProductQuery queryFactors ->
      evaluateProductQuery environment queryFactors

execute :: Environment -> Statement -> IO Stream
execute environment statement = case statement of
  SQuery query -> evaluateQuery environment query

