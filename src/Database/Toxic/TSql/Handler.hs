{-# LANGUAGE OverloadedStrings,TemplateHaskell #-}

module Database.Toxic.TSql.Handler where

import Control.Concurrent
import Control.Lens
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import qualified Data.Binary as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.Vector as V
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString

import Database.Toxic.TSql.Protocol

maximumMessageSize = 1000;

data HandlerState = HandlerState {
  _handlerSocket   :: Socket,
  _handlerNotify   :: Chan HandlerNotification,
  _handlerAction   :: Chan HandlerAction
  }

data HandlerNotification =
    NotificationNetworkSend BS.ByteString
  | NotificationNetworkReceive BS.ByteString
  deriving (Eq, Show)

data HandlerAction =
    ActionSendStartupMessage StartupMessage
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
  notifyChannel <- newChan
  actionChannel <- newChan
  let initialState = HandlerState {
        _handlerSocket = mySocket,
        _handlerNotify = notifyChannel,
        _handlerAction = actionChannel
        }
  return (error "handlerConnect: threadId shouldn't be used", initialState)

handleActions :: StateT HandlerState IO ()
handleActions = do
  state <- get
  actions <- lift $ getChanContents $ state ^. handlerAction
  mapM_ handleAction actions

handleAction :: HandlerAction -> StateT HandlerState IO ()
handleAction action = case action of
  ActionSendStartupMessage startupMessage -> processSendStartupMessage startupMessage

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

handlerSend :: BS.ByteString -> StateT HandlerState IO ()
handlerSend message = do
  state <- get
  let notifyChannel = state ^. handlerNotify
      socket = state ^. handlerSocket
  lift $ do
    writeChan notifyChannel $ NotificationNetworkSend message
    send socket message
    return ()

handlerReceive :: StateT HandlerState IO BS.ByteString
handlerReceive = do
  state <- get
  let notifyChannel = state ^. handlerNotify
      socket = state ^. handlerSocket
  lift $ do
    message <- recv socket maximumMessageSize
    writeChan notifyChannel $ NotificationNetworkReceive message
    return message

processSendStartupMessage :: StartupMessage -> StateT HandlerState IO ()
processSendStartupMessage startupMessage = do
  state <- get
  let message = startupMessage
      serializedMessage = BSL.toStrict $ B.runPut $ B.put message
  handlerSend serializedMessage
  handlerReceive >> return ()
