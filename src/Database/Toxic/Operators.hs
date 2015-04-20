module Database.Toxic.Operators where

import Database.Toxic.Types

valueType :: Value -> Type
valueType value = case value of
  VBool _ -> TBool
  VNull   -> TUnknown

arity2 :: (Value -> Value -> Value) -> (Value -> Value -> Value)
arity2 f a b =
  case (a, b) of
    (VNull, _) -> VNull
    (_, VNull) -> VNull
    _ -> f a b

operatorOr :: Value -> Value -> Value
operatorOr = arity2 $ \a b ->
  case (a, b) of
    (VBool x, VBool y) -> VBool $ x || y
    -- TODO: Switch to dedicated error type for this
    _ -> error $ "or: Incorrect types"
