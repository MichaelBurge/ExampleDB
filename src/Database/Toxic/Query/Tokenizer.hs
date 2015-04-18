module Database.Toxic.Query.Tokenizer where

import Control.Applicative ((*>))
import Text.Parsec

type CharParser a = Parsec String () a

data SourcePosition = SourcePosition { line, column :: Int } deriving (Eq, Show)

data Token =
     TkSelect
   | TkTrue
   | TkFalse
   | TkStatementEnd
   deriving (Eq, Show)

lexStatementEnd :: CharParser Token
lexStatementEnd = char ';' *> return TkStatementEnd

lexKeyword :: CharParser Token
lexKeyword =
  let tryKeyword keyword token = string keyword *> spaces *> return token
  in     tryKeyword "select" TkSelect
     <|> tryKeyword "true" TkTrue
     <|> tryKeyword "false" TkFalse

lexOneToken :: CharParser Token
lexOneToken =
        lexStatementEnd
    <|> lexKeyword

lexer :: CharParser [ Token ]
lexer = many lexOneToken

lex :: String -> Either ParseError [Token]
lex source = parse lexer "" source
