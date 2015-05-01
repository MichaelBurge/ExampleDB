module Database.Toxic.Operators where

import Database.Toxic.Types

valueType :: Value -> Type
valueType value = case value of
  VBool _ -> TBool
  VInt _ -> TInt
  VNull   -> TUnknown

arity1 :: (Value -> Value) -> (Value -> Value)
arity1 f a =
  case a of
    VNull -> VNull
    _ -> f a

arity2 :: (Value -> Value -> Value) -> (Value -> Value -> Value)
arity2 f a b =
  case (a, b) of
    (VNull, _) -> VNull
    (_, VNull) -> VNull
    _ -> f a b

operatorNot :: Value -> Value
operatorNot = arity1 $ \a ->
  case a of
    VBool x -> VBool $ not x
    _ -> error $ "operatorNot: Incorrect type"

operatorOr :: Value -> Value -> Value
operatorOr = arity2 $ \a b ->
  case (a, b) of
    (VBool x, VBool y) -> VBool $ x || y
    -- TODO: Switch to dedicated error type for this
    _ -> error $ "operatorOr: Incorrect types"

operatorPlus :: Value -> Value -> Value
operatorPlus = arity2 $ \a b ->
  case (a, b) of
    (VInt x, VInt y) -> VInt $ x + y
    _ -> error $ "operatorPlus: Incorrect types"

operatorMinus :: Value -> Value -> Value
operatorMinus = arity2 $ \a b ->
  case (a, b) of
    (VInt x, VInt y) -> VInt $ x - y
    _ -> error $ "OperatorMinus: Incorrect types"

operatorTimes :: Value -> Value -> Value
operatorTimes = arity2 $ \a b ->
  case (a, b) of
    (VInt x, VInt y) -> VInt $ x * y
    _ -> error $ "operatorTimes: Incorrect types"
  
operatorDividedBy :: Value -> Value -> Value
operatorDividedBy = arity2 $ \a b ->
  case (a, b) of
    (VInt x, VInt 0) -> VNull
    (VInt x, VInt y) -> VInt $ x `div` y
    _ -> error $ "operatorDividedBy: Incorrect types"

-- TODO: This operator mixes the 'null as uninitialized' with 'null as SQL value'.
-- There should probably be a separate 'unitialized' value.
operatorFailUnlessNull :: Value -> Value -> Value
operatorFailUnlessNull a b =
  case (a, b) of
    (x, VNull) -> x
    (x, y) | x == y-> x
    x -> error $
         "operatorFailUnlessNull: Attempted to combine non-null values " ++ show x
