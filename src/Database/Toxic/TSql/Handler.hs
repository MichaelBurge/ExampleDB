{-# LANGUAGE OverloadedStrings,TemplateHaskell #-}

module Database.Toxic.TSql.Handler where

import Control.Lens
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import qualified Data.ByteString.Char8 as BS
import qualified Data.Vector as V
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString

import Database.Toxic.TSql.Protocol

data HandleState = HandleState {
  _handleSocket :: Socket
  } deriving (Eq, Show)

makeLenses ''HandleState

connect :: StateT HandleState IO ()
connect = do
  let family = AF_UNIX
      socketType = Stream
      protocolNumber = defaultProtocol
  mySocket <- lift $ socket family socketType protocolNumber
  modify (& handleSocket .~ mySocket)

startupMessage :: StartupMessage
startupMessage = StartupMessage {
  startupMessageParameters = V.fromList [
     ("user", "mburge"),
     ("database", "mburge"),
     ("application_name", "psql"),
     ("client_encoding", "UTF8")
     ]
  }
                                
-- sendStartupMessage :: StateT HandleState IO ()
-- sendStartupMessage = do
--   state <- get
--   let message = startupMessage
--       serializedMessage = runPut 
