{-# LANGUAGE OverloadedStrings #-}

module Database.Toxic.TSql.ProtocolHelper where

import Database.Toxic.Query.AST as Q
import Database.Toxic.Query.Parser as Q
import Database.Toxic.TSql.Protocol as P
import Database.Toxic.Types

import Control.Lens
import Data.Char
import qualified Data.ByteString.Char8 as BS
import qualified Data.Vector as V
import qualified Data.Text as T

serializeType :: Type -> RowDescriptionField
serializeType TBool = RowDescriptionField {
  _rowDescriptionFieldName = error "serializeType: No name given",
  _rowDescriptionFieldOid = 0,
  _rowDescriptionFieldAttributeNumber = 0,
  _rowDescriptionFieldDataType = 16, -- _bool
  _rowDescriptionFieldSize = 1,
  _rowDescriptionFieldModifier = fromIntegral $ -1,
  _rowDescriptionFieldFormatCode = 0 -- text
  }
serializeType TInt =
  RowDescriptionField {
  _rowDescriptionFieldName = error "serializeType: No name given",
  _rowDescriptionFieldOid = 0,
  _rowDescriptionFieldAttributeNumber = 0,
  _rowDescriptionFieldDataType = 23, -- _int4
  _rowDescriptionFieldSize = 4,
  _rowDescriptionFieldModifier = fromIntegral $ -1,
  _rowDescriptionFieldFormatCode = 0 -- text
  }
serializeType TUnknown =
  RowDescriptionField {
    _rowDescriptionFieldName = error "serializeType: No name given",
    _rowDescriptionFieldOid = 0,
    _rowDescriptionFieldAttributeNumber = 0,
    _rowDescriptionFieldDataType = 0, -- unknown
    _rowDescriptionFieldSize = fromIntegral $ -2,
    _rowDescriptionFieldModifier = fromIntegral $ -1,
    _rowDescriptionFieldFormatCode = 0 -- text
    }
serializeColumn :: Column -> RowDescriptionField
serializeColumn column =
  (serializeType $ columnType column)
  & rowDescriptionFieldName .~ (BS.pack $ T.unpack $ columnName column)

serializeValue :: Value -> Maybe BS.ByteString
serializeValue value = case value of
  VBool x -> Just $ BS.pack $ show x
  VInt x -> Just $ BS.pack $ show x
  VNull -> Nothing

serializeRow :: Record -> DataRow
serializeRow record =
  case record of
    Record vs -> DataRow {
      dataRowValues = V.map serializeValue vs
      }

serializeStream :: Stream -> [AnyMessage]
serializeStream stream = [
  MRowDescription RowDescription {
     rowDescriptionFields = V.map serializeColumn $ streamHeader stream
     }
  ] ++
  (map (MDataRow . serializeRow) (streamRecords stream)) ++ [
  MCommandComplete CommandComplete {
     commandCompleteTag = BS.pack $ "SELECT " ++ show (length $ streamRecords stream)
     },
  MReadyForQuery ReadyForQuery {
    readyForQueryStatus = fromIntegral $ ord 'I'
    }
  ]

serializeError :: QueryError -> ErrorResponse
serializeError queryError =
  let errorMessage = show queryError
      messageBs = BS.pack $ errorMessage
  in ErrorResponse {
    errorResponseTypesAndValues = V.fromList $
                                  map (\(x,y) -> (fromIntegral $ ord x, y)) [
       ('S', "ERROR"),
       ('M', messageBs)
       ]
    }

deserializeQuery :: P.Query -> Either QueryError Q.Query
deserializeQuery query =
  let queryText = T.pack $ BS.unpack $ queryQuery query
  in case runQueryParser queryText of
    Left parseError -> Left $
                       ErrorParseError $
                       T.pack $
                       show parseError
    Right (SQuery parsedQuery) -> Right parsedQuery
