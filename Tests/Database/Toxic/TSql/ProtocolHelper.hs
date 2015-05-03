{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.TSql.ProtocolHelper (protocolHelperTests) where

import Database.Toxic.TSql.Protocol
import Database.Toxic.TSql.ProtocolHelper
import Database.Toxic.Streams
import Database.Toxic.Types

import Control.Lens
import Data.Char

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

import qualified Data.Vector as V

test_int4_stream :: Assertion
test_int4_stream =
  let column = Column { columnName = "example", columnType = TInt }
      stream = singleton_stream column $ VInt 5
      actualMessages = serializeStream stream
      expectedMessages = [
        MRowDescription RowDescription {
           rowDescriptionFields = V.singleton RowDescriptionField {
              _rowDescriptionFieldName = "example",
              _rowDescriptionFieldOid = 0,
              _rowDescriptionFieldAttributeNumber = 0,
              _rowDescriptionFieldDataType = 23,
              _rowDescriptionFieldSize = 4,
              _rowDescriptionFieldModifier = fromIntegral $ -1,
              _rowDescriptionFieldFormatCode = 0
              }
           },
        MDataRow DataRow {
          dataRowValues = V.singleton $ Just "5"
          },
        MCommandComplete CommandComplete {
          commandCompleteTag = "SELECT 1"
          }
        ]
  in assertEqual "" expectedMessages actualMessages

protocolHelperTests =
  testGroup "Protocol Helper" [
    testCase "int4 stream" test_int4_stream
    ]
