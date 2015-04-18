module Database.Toxic.Types where

import Data.Array.Unboxed
import Data.Text

type ArrayOf a = UArray Int a
type SetOf a = [ a ]
  
data Value =
    VBool Bool

data Type =
    TBool
  | TProduct (ArrayOf Type)

data Column = Column {
  columnName :: Text,
  columnType :: Type
  }

data Record = ArrayOf Value

data Table = Table {
  tableName       :: Text,
  tableRecordType :: Type,
  tableRecords    :: SetOf Record
  }


