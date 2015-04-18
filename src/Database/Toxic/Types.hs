module Database.Toxic.Types where

import Data.Text
import Data.Vector

type ArrayOf a = Vector a
type SetOf a = [ a ]
  
data Value =
    VBool Bool

data Type =
    TBool

data Column = Column {
  columnName :: Text,
  columnType :: Type
  }

data Record = Record (ArrayOf Value)
data Stream = Stream {
  streamHeader  :: ArrayOf Column,
  streamRecords :: SetOf Record
  }

data Table = Table {
  tableName   :: Text,
  tableStream :: Stream
  }


