module Database.Toxic.Operators where

import Control.Applicative

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

packBool :: Maybe Bool -> Value
packBool (Just x) = VBool x
packBool Nothing = VNull

unpackBool :: Value -> Maybe Bool
unpackBool x = case x of
  VBool y -> Just y
  _ -> Nothing

packInt :: Maybe Int -> Value
packInt (Just x) = VInt x
packInt Nothing = VNull

unpackInt :: Value -> Maybe Int
unpackInt (VInt x) = Just x
unpackInt _ = Nothing

packValue :: Maybe Value -> Value
packValue (Just x) = x
packValue Nothing = VNull

unpackValue :: Value -> Maybe Value
unpackValue x = Just x

wrapHaskellUnop :: (a -> a) -> (Value -> Maybe a) -> (Maybe a -> Value) -> (Value -> Value)
wrapHaskellUnop f unpack pack value = pack $ (f <$> unpack value)

wrapHaskellBinop :: (a -> a -> b) -> (Value -> Maybe a) -> (Maybe b -> Value) -> (Value -> Value -> Value)
wrapHaskellBinop f unpack pack x1 x2 = pack $ (f <$> unpack x1 <*> unpack x2)

operatorNot = wrapHaskellUnop not unpackBool packBool
operatorOr = wrapHaskellBinop (||) unpackBool packBool
operatorPlus = wrapHaskellBinop (+) unpackInt packInt
operatorMinus = wrapHaskellBinop (-) unpackInt packInt
operatorTimes = wrapHaskellBinop (*) unpackInt packInt
operatorDividedBy = wrapHaskellBinop div unpackInt packInt
operatorUnequal = wrapHaskellBinop (/=) unpackValue packBool
operatorGreater = wrapHaskellBinop (>) unpackValue packBool
operatorGreaterOrEqual = wrapHaskellBinop (>=) unpackValue packBool
operatorEqual = wrapHaskellBinop (==) unpackValue packBool
operatorLessOrEqual = wrapHaskellBinop (<=) unpackValue packBool
operatorLess = wrapHaskellBinop (<) unpackValue packBool

-- TODO: This operator mixes the 'null as uninitialized' with 'null as SQL value'.
-- There should probably be a separate 'unitialized' value.
operatorFailUnlessNull :: Value -> Value -> Value
operatorFailUnlessNull a b =
  case (a, b) of
    (x, VNull) -> x
    (x, y) | x == y-> x
    x -> error $
         "operatorFailUnlessNull: Attempted to combine non-null values " ++ show x
