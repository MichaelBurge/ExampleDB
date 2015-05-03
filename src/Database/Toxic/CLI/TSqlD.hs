{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

module Database.Toxic.CLI.TSqlD where

import Database.Toxic.Query.AST as Q
import Database.Toxic.Query.Interpreter as Q
import Database.Toxic.Query.Parser as Q
import Database.Toxic.TSql.Protocol as P
import Database.Toxic.TSql.ProtocolHelper as P

import Control.Concurrent
import Control.Exception
import Control.Lens
import Control.Monad
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import qualified Data.Binary as B
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Char
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import System.Directory

data SessionState = SessionState {
  _sessionClientSocket :: Socket,
  _sessionStateMessage :: Decoder AnyMessage,
  _sessionEnvironment  :: Environment,
  _sessionExit         :: Bool
  }

makeLenses ''SessionState

maximumReceiveLength = 10000

socketAddress = "/home/mburge/tmp/.s.PGSQL.5432"

serverConnect :: IO Socket
serverConnect = do
  let family = AF_UNIX
      socketType = Stream
      protocolNumber = defaultProtocol
      address = SockAddrUnix socketAddress
  mySocket <- socket family socketType protocolNumber
  bindSocket mySocket address
  listen mySocket 1
  return mySocket

serverSendMessage :: AnyMessage -> StateT SessionState IO ()
serverSendMessage message = do
  lift $ putStrLn $ "Sending message: " ++ show message
  sessionState <- get
  let clientSocket = sessionState ^. sessionClientSocket
      networkMessage = BSL.toStrict $ B.encode message
  lift $ putStrLn $ "Sending bytes: " ++ show networkMessage
  lift $ send clientSocket networkMessage
  return ()

handleStartupMessage :: StartupMessage -> StateT SessionState IO ()
handleStartupMessage message = do
  serverSendMessage $ MAuthenticationOk AuthenticationOk
  let sendParameter name value =
        serverSendMessage $ MParameterStatus $ ParameterStatus name value
  sendParameter "application_name" "psql"
  sendParameter "client_encoding" "UTF8"
  sendParameter "DateStyle" "ISO, MDY"
  sendParameter "integer_datetimes" "on"
  sendParameter "IntervalStyle" "postgres"
  sendParameter "is_superuser" "on"
  sendParameter "server_encoding" "UTF8"
  sendParameter "server_version" "9.4.1"
  sendParameter "session_authorization" "mburge"
  sendParameter "standard_conforming_strings" "on"
  sendParameter "TimeZone" "localtime"
  serverSendMessage $ MBackendKeyData BackendKeyData {
    backendKeyDataProcessId = 0,
    backendKeyDataSecretKey = 0
    }
  serverSendReadyForQuery

serverSendReadyForQuery :: StateT SessionState IO ()
serverSendReadyForQuery = serverSendMessage $ MReadyForQuery ReadyForQuery {
  readyForQueryStatus = fromIntegral $ ord 'I'
  }

serverSendError :: QueryError -> StateT SessionState IO ()
serverSendError queryError = do
  serverSendMessage $
    MErrorResponse $
    serializeError queryError
  serverSendReadyForQuery

handleQuery :: P.Query -> StateT SessionState IO ()
handleQuery query = case deserializeQuery query of
  Left queryError -> serverSendError queryError
  Right parsedQuery -> do
    sessionState <- get
    let environment = sessionState ^. sessionEnvironment
    queryResults <- lift $ executeQuery environment parsedQuery
    case queryResults of
      Left queryError -> serverSendError queryError
      Right queryResults -> do
        let messages = serializeStream queryResults
        forM_ messages serverSendMessage
        serverSendReadyForQuery

handleTerminate :: Terminate -> StateT SessionState IO ()
handleTerminate query = modify (& sessionExit .~ True)

handleMessage :: AnyMessage -> StateT SessionState IO ()
handleMessage message = case message of
  MStartupMessage x -> handleStartupMessage x
  MQuery x -> handleQuery x
  MTerminate x -> handleTerminate x

handleNewInput :: BS.ByteString -> StateT SessionState IO ()
handleNewInput bs = do
  state <- get
  let decoder = state ^. sessionStateMessage
      newDecoder = pushEndOfInput $
                   pushChunk decoder bs
  modify $ (& sessionStateMessage .~ newDecoder)
  case newDecoder of
    Fail unconsumed offset errorMessage ->
      error $ "Failed to decode:" ++ show (unconsumed, offset, errorMessage)
    Partial f -> do
      lift $ putStrLn $ "Partial decoder"
      return ()
    Done unconsumed offset message -> do
      lift $ putStrLn $ "Interpreted message: " ++ show message
      handleMessage message
      modify $ (& sessionStateMessage .~ runGetIncremental B.get)
      if BS.null unconsumed
        then return ()
        else handleNewInput unconsumed

serverHandler :: (Socket, SockAddr) -> IO ()
serverHandler (clientSocket, clientAddress) = do
  let initialState = SessionState {
        _sessionClientSocket = clientSocket,
        _sessionStateMessage = runGetIncremental B.get,
        _sessionEnvironment = Environment { },
        _sessionExit = False
        }
  evalStateT (loop clientSocket clientAddress) initialState
  where
    loop :: Socket -> SockAddr -> StateT SessionState IO ()
    loop clientSocket clientAddress = do
      networkMessage <- lift $ recv clientSocket maximumReceiveLength
      lift $ putStrLn $ "Received bytes: " ++ show networkMessage
      handleNewInput networkMessage
      state <- get
      if state ^. sessionExit
        then return ()
        else loop clientSocket clientAddress

cleanup :: Socket -> IO ()
cleanup socket = do
  close socket
  removeFile socketAddress

tsqldMain :: IO ()
tsqldMain = bracket serverConnect cleanup serverLoop
  where
    serverLoop socket = do
      bracket (accept socket) (close . fst) serverHandler
      serverLoop socket
