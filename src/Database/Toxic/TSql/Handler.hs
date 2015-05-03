{-# LANGUAGE OverloadedStrings,TemplateHaskell #-}

module Database.Toxic.TSql.Handler where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM.TChan
import Control.Lens
import Control.Monad.STM
import Control.Monad.Trans
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import qualified Data.Binary as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.Vector as V
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import System.Posix.Types

import Database.Toxic.TSql.Protocol

maximumMessageSize = 1000

data HandlerState = HandlerState {
  _handlerSocket   :: Socket
  }

data HandlerNotification =
    NotificationNetworkSend BS.ByteString
  | NotificationNetworkReceive BS.ByteString
  deriving (Eq, Show)

data HandlerAction =
    ActionSendStartupMessage StartupMessage
  | ActionSendQuery BS.ByteString
  deriving (Eq, Show)

makeLenses ''HandlerState

handlerConnect :: IO (ThreadId, HandlerState)
handlerConnect = do
  let family = AF_UNIX
      socketType = Stream
      protocolNumber = defaultProtocol
      address = SockAddrUnix "/var/run/postgresql/.s.PGSQL.5432"
  mySocket <- socket family socketType protocolNumber
  connect mySocket address
  let initialState = HandlerState {
        _handlerSocket = mySocket
        }
  return (error "handlerConnect: threadId shouldn't be used", initialState)

handleAction :: HandlerState -> HandlerAction -> IO ()
handleAction state action = case action of
  ActionSendStartupMessage startupMessage ->
    processSendStartupMessage state startupMessage
  ActionSendQuery query ->
    processSendQuery state query

defaultStartupMessage :: StartupMessage
defaultStartupMessage = StartupMessage {
  startupMessageProtocolVersion = defaultProtocolVersion,
  startupMessageParameters = V.fromList [
     ("user", "mburge"),
     ("database", "mburge"),
     ("application_name", "psql"),
     ("client_encoding", "UTF8")
     ]
  }

handlerSend :: HandlerState -> BS.ByteString -> IO ()
handlerSend state message = do
  let socket = state ^. handlerSocket
  putStrLn $ "Sent message: " ++ show message
  send socket message
  return ()

waitOnSocket socket = do
  putStrLn "Waiting on socket"
  threadWaitRead $Fd $ fdSocket socket

handlerReceive :: HandlerState -> IO BS.ByteString
handlerReceive state = do
  let socket = state ^. handlerSocket
  waitOnSocket socket
  message <- recv socket maximumMessageSize
  putStrLn $ "Received message: " ++ show message
  return message

processSendStartupMessage :: HandlerState -> StartupMessage -> IO ()
processSendStartupMessage state startupMessage = do
  let message = startupMessage
      serializedMessage = BSL.toStrict $ B.runPut $ B.put message
  handlerSend state serializedMessage
  handlerReceive state >> return ()

processSendQuery :: HandlerState -> BS.ByteString -> IO ()
processSendQuery state message = do
  let query = Query { queryQuery = message }
      serializedMessage = BSL.toStrict $ B.runPut $ B.put query
  handlerSend state serializedMessage
  handlerReceive state >> return ()
