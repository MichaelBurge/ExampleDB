module Database.Toxic.CLI.TSql where

import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.State.Strict
import qualified Data.Text as T
import System.Console.Haskeline

import Database.Toxic.TSql.AST
import Database.Toxic.TSql.Parser

data InterpreterState = InterpreterState { } deriving (Eq, Show)

mkInterpreterState :: InterpreterState
mkInterpreterState = error "mkInterpreterState: Implement me!"

interpreterSettings :: MonadIO m => Settings m
interpreterSettings = defaultSettings {
  autoAddHistory = True,
  historyFile = Just ".tsql_history"
  }

runCommand :: Command -> StateT InterpreterState (InputT IO) ()
runCommand command = case command of
  CStatement statement -> lift $ outputStrLn $ "Received command: " ++ show command

tsqlMain :: IO ()
tsqlMain =
  runInputT interpreterSettings $
  evalStateT loop mkInterpreterState
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
