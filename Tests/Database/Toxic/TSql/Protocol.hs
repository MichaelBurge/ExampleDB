{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.TSql.Protocol (protocolTests) where

import Database.Toxic.TSql.Protocol

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Char8
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Char
import qualified Data.Vector as V

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

assertSerialization :: AnyMessage -> BSL.ByteString -> Assertion
assertSerialization message expectedSerialized =
  let actualSerialized = encode message
      unserialized = decode actualSerialized
  in do
    assertEqual "put" expectedSerialized actualSerialized
    assertEqual "get" message unserialized


test_startup_message :: Assertion
test_startup_message =
  let message = MStartupMessage StartupMessage {
        startupMessageProtocolVersion = defaultProtocolVersion,
        startupMessageParameters = V.fromList [
           ("user", "mburge"),
           ("database", "mburge"),
           ("application_name", "psql"),
           ("client_encoding", "UTF8")
           ]
        }
      -- Observed from strace'ing psql
      expectedSerialized = "\0\0\0P\0\3\0\0user\0mburge\0database\0mburge\0application_name\0psql\0client_encoding\0UTF8\0\0"
  in assertSerialization message expectedSerialized

test_query :: Assertion
test_query =
  let message = MQuery Query {
        queryQuery = "select 5;"
        }
      -- Observed from strace'ing psql
      expectedSerialized = "\x51\x00\x00\x00\x0e\x73\x65\x6c\x65\x63\x74\x20\x35\x3b\x00"
  in assertSerialization message expectedSerialized
      
test_authentication_ok :: Assertion
test_authentication_ok =
  let message = MAuthenticationOk AuthenticationOk
      expectedSerialized :: BSL.ByteString
      expectedSerialized = "\x52\x00\x00\x00\x08\x00\x00\x00\x00"
  in assertSerialization message expectedSerialized

test_parameter_status :: Assertion
test_parameter_status =
  let message = MParameterStatus ParameterStatus {
        parameterStatusName = "application_name",
        parameterStatusValue = "psql"
        }
      expectedSerialized = "\x53\x00\x00\x00\x1a\x61\x70\x70\x6c\x69\x63\x61\x74\x69\x6f\x6e\x5f\x6e\x61\x6d\x65\x00\x70\x73\x71\x6c\x00"
  in assertSerialization message expectedSerialized

test_backend_key_data :: Assertion
test_backend_key_data =
  let message = MBackendKeyData BackendKeyData {
        backendKeyDataProcessId = 13143,
        backendKeyDataSecretKey = 677778168
        }
      expectedSerialized :: BSL.ByteString
      expectedSerialized = "\x4b\x00\x00\x00\x0c\x00\x00\x33\x57\x28\x66\x12\xf8"
  in assertSerialization message expectedSerialized

test_ready_for_query :: Assertion
test_ready_for_query =
  let message = MReadyForQuery ReadyForQuery {
        readyForQueryStatus = fromIntegral $ ord 'I'
        }
      expectedSerialized = "\x5a\x00\x00\x00\x05\x49"
  in assertSerialization message expectedSerialized

test_row_description :: Assertion
test_row_description =
  let message = MRowDescription RowDescription {
        rowDescriptionFields = V.singleton RowDescriptionField {
           rowDescriptionFieldName = "?column?",
           rowDescriptionFieldOid = 0,
           rowDescriptionFieldAttributeNumber = 0,
           rowDescriptionFieldDataType = 23, -- _int4
           rowDescriptionFieldSize = 4,
           rowDescriptionFieldModifier = fromIntegral $ -1,
           rowDescriptionFieldFormatCode = 0 -- text
           }
        }
      expectedSerialized = "\x54\x00\x00\x00\x21\x00\x01\x3f\x63\x6f\x6c\x75\x6d\x6e\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00"
  in assertSerialization message expectedSerialized

expectedSerialized = "\x44\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x01\x35\x43\x00\x00\x00\x0d\x53\x45\x4c\x45\x43\x54\x20\x31\x00\x5a\x00\x00\x00\x05\x49"

protocolTests =
  testGroup "Protocol" [
    testCase "Startup message" test_startup_message,
    testCase "Query" test_query,
    testCase "Authentication Ok" test_authentication_ok,
    testCase "Parameter Status" test_parameter_status,
    testCase "Backend Key Data" test_backend_key_data,
    testCase "Ready For Query" test_ready_for_query,
    testCase "Row Description" test_row_description
    ]
