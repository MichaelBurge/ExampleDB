module Database.Toxic.CLI.TSqlD where

import Database.Toxic.TSql.Protocol

import Control.Concurrent
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import Data.Binary
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString

data SessionState = SessionState { } deriving (Eq, Show)

maximumReceiveLength = 10000

serverConnect :: IO Socket
serverConnect = do
  let family = AF_UNIX
      socketType = Stream
      protocolNumber = defaultProtocol
      address = SockAddrUnix "/home/mburge/tmp/.s.PGSQL.5432"
  mySocket <- socket family socketType protocolNumber
  bindSocket mySocket address
  listen mySocket 1
  return mySocket

handleStartupMessage :: StartupMessage -> StateT SessionState IO ()
handleStartupMessage message = return ()

handleQuery :: Query -> StateT SessionState IO ()
handleQuery query = return ()

handleMessage :: AnyMessage -> StateT SessionState IO ()
handleMessage message = case message of
  MStartupMessage x -> handleStartupMessage x
  MQuery x -> handleQuery x

serverHandler :: (Socket, SockAddr) -> IO ()
serverHandler (clientSocket, clientAddress) = do
  let initialState = SessionState
  evalStateT (loop clientSocket clientAddress) initialState
  where
    loop :: Socket -> SockAddr -> StateT SessionState IO ()
    loop clientSocket clientAddress = do
      networkMessage <- lift $ recv clientSocket maximumReceiveLength
      let message = decode $ BSL.fromStrict networkMessage :: AnyMessage
      handleMessage message
      loop clientSocket clientAddress

tsqldMain :: IO ()
tsqldMain = do
  serverSocket <- serverConnect
  serverLoop serverSocket
  where
    serverLoop socket = do
      (clientSocket, clientAddress) <- accept socket
      forkIO $ serverHandler (clientSocket, clientAddress)
      serverLoop socket
