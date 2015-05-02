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
      chanNotify = handlerState ^. handlerNotify
  writeChan chanAction $ ActionSendStartupMessage defaultStartupMessage
  forkIO $ evalStateT handleActions handlerState
  forkIO $ handleNotifications chanNotify
  return InterpreterState {
    _interpreterHandlerState = handlerState,
    _interpreterHandlerThread = handlerThread
    }

interpreterSettings :: MonadIO m => Settings m
interpreterSettings = defaultSettings {
  autoAddHistory = True,
  historyFile = Just ".tsql_history"
  }

handleNotification :: HandlerNotification -> IO ()
handleNotification notification = case notification of
  NotificationNetworkSend bs -> putStrLn $ "Sent bytes: " ++ show bs
  NotificationNetworkReceive bs -> putStrLn $ "Received bytes: " ++ show bs

handleNotifications :: Chan HandlerNotification -> IO ()
handleNotifications chanNotify = do
  notifications <- liftIO $ getChanContents chanNotify
  mapM_ handleNotification notifications

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
