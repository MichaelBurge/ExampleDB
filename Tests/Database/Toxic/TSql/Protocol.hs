{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.TSql.Protocol (protocolTests) where

import Database.Toxic.TSql.Protocol

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Char8
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Vector as V

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

test_startup_message :: Assertion
test_startup_message =
  let message = StartupMessage {
        startupMessageProtocolVersion = defaultProtocolVersion,
        startupMessageParameters = V.fromList [
           ("user", "mburge"),
           ("database", "mburge"),
           ("application_name", "psql"),
           ("client_encoding", "UTF8")
           ]
        }
      -- Observed from strace'ing psql
      expectedSerialized :: BSL.ByteString
      expectedSerialized = "\0\0\0P\0\3\0\0user\0mburge\0database\0mburge\0application_name\0psql\0client_encoding\0UTF8\0\0"
      actualSerialized :: BSL.ByteString
      actualSerialized = runPut $ put message
      unserialized :: StartupMessage
      unserialized = runGet get actualSerialized

      anyMessage = MStartupMessage message
      serializedAnyMessage = runPut $ put anyMessage :: BSL.ByteString
      unserializedMessage = runGet get expectedSerialized :: AnyMessage
  in do
     assertEqual "put" expectedSerialized actualSerialized
     assertEqual "get" message unserialized
     assertEqual "put message" expectedSerialized serializedAnyMessage
     assertEqual "get message" anyMessage unserializedMessage

test_query :: Assertion
test_query =
  let message = Query {
        queryQuery = "select 5;"
        }
      -- Observed from strace'ing psql
      expectedSerialized :: BSL.ByteString
      expectedSerialized = "\x51\x00\x00\x00\x0e\x73\x65\x6c\x65\x63\x74\x20\x35\x3b\x00"
      actualSerialized = runPut $ put message
      unserialized = runGet get actualSerialized
  in do
    assertEqual "BSL Length" (14 + 1) $ BSL.length expectedSerialized
    assertEqual "put" expectedSerialized actualSerialized
    assertEqual "get" message unserialized

test_authentication_ok :: Assertion
test_authentication_ok =
  let message = MAuthenticationOk AuthenticationOk
      expectedSerialized :: BSL.ByteString
      expectedSerialized = "\x52\x00\x00\x00\x08\x00\x00\x00\x00"
      actualSerialized = runPut $ put message
      unserialized = runGet get actualSerialized
  in do
    assertEqual "put" expectedSerialized actualSerialized
    assertEqual "get" message unserialized

protocolTests =
  testGroup "Protocol" [
    testCase "Startup message" test_startup_message,
    testCase "Query" test_query,
    testCase "Authentication Ok" test_authentication_ok
    ]
