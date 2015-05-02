{-# LANGUAGE TemplateHaskell #-}

module Database.Toxic.CLI.TSql where

import Control.Concurrent
import Control.Lens
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.State
import qualified Data.Text as T
import System.Console.Haskeline

import Database.Toxic.TSql.AST
import Database.Toxic.TSql.Handler
import Database.Toxic.TSql.Parser
import Database.Toxic.TSql.Protocol

data InterpreterState = InterpreterState {
  _interpreterHandlerState :: HandlerState,
  _interpreterHandlerThread :: ThreadId
  }

makeLenses ''InterpreterState

mkInterpreterState :: IO InterpreterState
mkInterpreterState = do
  (handlerThread, handlerState) <- handlerConnect
  let chanAction = handlerState ^. handlerAction
  writeChan chanAction $ ActionSendStartupMessage defaultStartupMessage
  return InterpreterState {
    _interpreterHandlerState = handlerState,
    _interpreterHandlerThread = handlerThread
    }

interpreterSettings :: MonadIO m => Settings m
interpreterSettings = defaultSettings {
  autoAddHistory = True,
  historyFile = Just ".tsql_history"
  }

handleNotification :: HandlerNotification -> StateT InterpreterState (InputT IO) ()
handleNotification notification = case notification of
  NotificationNetworkSend bs -> lift $ outputStrLn $ "Sent bytes: " ++ show bs
  NotificationNetworkReceive bs -> lift $ outputStrLn $ "Received bytes: " ++ show bs

handleNotifications :: StateT InterpreterState (InputT IO) ()
handleNotifications = do
  state <- get
  let chan = state ^. interpreterHandlerState ^. handlerNotify
  notifications <- liftIO $ getChanContents chan
  mapM_ handleNotification notifications

runHandler :: StateT InterpreterState (InputT IO) ()
runHandler = do
  state <- get
  liftIO $ evalStateT handlerLoop $ state ^. interpreterHandlerState

runCommand :: Command -> StateT InterpreterState (InputT IO) ()
runCommand command = case command of
  CStatement statement -> lift $ outputStrLn $ "Received command: " ++ show command

tsqlMain :: IO ()
tsqlMain = do
  defaultInterpreterState <- mkInterpreterState
  runInputT interpreterSettings $
    evalStateT loop defaultInterpreterState
  where
    loop :: StateT InterpreterState (InputT IO) ()
    loop = do
      handleNotifications
      runHandler
      minput <- lift $ getInputLine "> "
      case minput of
        Nothing -> return ()
        Just "\\q" -> return ()
        Just input -> do
          case runCommandParser $ T.pack input of
            Left parse_error -> lift $ outputStrLn $
                                "Parse error: " ++ show parse_error
            Right command -> runCommand command
          
          loop
