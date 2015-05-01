{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Toxic.TSql.Protocol (protocolTests) where

import Database.Toxic.TSql.Protocol

import Data.Binary
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
        startupMessageParameters = V.fromList [
           ("user", "mburge"),
           ("database", "mburge"),
           ("application_name", "psql"),
           ("client_encoding", "UTF8")
           ]
        }
      -- Observed from strace'ing psql
      expectedSerialized :: BS.ByteString
      expectedSerialized = "\0\0\0P\0\3\0\0user\0mburge\0database\0mburge\0application_name\0psql\0client_encoding\0UTF8\0\0"
      actualSerialized :: BS.ByteString
      actualSerialized = BSL.toStrict $ runPut $ put message
  in assertEqual "" expectedSerialized actualSerialized

protocolTests =
  testGroup "Protocol" [
    testCase "Startup message" test_startup_message
    ]
