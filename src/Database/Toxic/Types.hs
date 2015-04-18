module Database.Toxic.Types where

import Data.Text
import Data.Vector

type ArrayOf a = Vector a
type SetOf a = [ a ]
  
data Value =
      VBool Bool
    | VNull
    deriving (Eq, Show)

data Type =
    TBool
    deriving (Eq, Show)

data Column = Column {
  columnName :: Text,
  columnType :: Type
  } deriving (Eq, Show)

data Record = Record (ArrayOf Value) deriving (Eq, Show)
data Stream = Stream {
  streamHeader  :: ArrayOf Column,
  streamRecords :: SetOf Record
  } deriving (Eq, Show)

data Table = Table {
  tableName   :: Text,
  tableStream :: Stream
  } deriving (Eq, Show)


