module Database.Toxic.Query.Tokenizer where

import Control.Applicative ((<$>), (*>), (<*>))
import qualified Data.Text as T
import Text.Parsec

type CharParser a = Parsec String () a

data SourcePosition = SourcePosition { line, column :: Int } deriving (Eq, Show)

data Token =
  -- Keywords
     TkSelect
   | TkTrue
   | TkFalse
   | TkRename
   | TkIdentifier T.Text
   | TkCase
   | TkWhen
   | TkThen
   | TkElse
   | TkEnd
   | TkUnion
   | TkFrom

  -- Symbols
   | TkStatementEnd -- ;
   | TkOpen         -- (
   | TkClose        -- )
   | TkSequence     -- ,
   deriving (Eq, Show)

lexStatementEnd :: CharParser Token
lexStatementEnd = char ';' *> return TkStatementEnd

lexSymbol :: CharParser Token
lexSymbol =
  let trySymbol c token = try (char c *> spaces *> return token)
  in     trySymbol ';' TkStatementEnd
     <|> trySymbol '(' TkOpen
     <|> trySymbol ')' TkClose
     <|> trySymbol ',' TkSequence

lexKeyword :: CharParser Token
lexKeyword =
  let tryKeyword keyword token = try (string keyword *> spaces *> return token)
  in     tryKeyword "select" TkSelect
     <|> tryKeyword "true" TkTrue
     <|> tryKeyword "false" TkFalse
     <|> tryKeyword "as" TkRename
     <|> tryKeyword "case" TkCase
     <|> tryKeyword "when" TkWhen
     <|> tryKeyword "then" TkThen
     <|> tryKeyword "else" TkElse
     <|> tryKeyword "end" TkEnd
     <|> tryKeyword "union" TkUnion
     <|> tryKeyword "from" TkFrom

lexIdentifier :: CharParser Token
lexIdentifier =
  let chars = (:) <$> letter <*> many alphaNum
  in TkIdentifier <$> T.pack <$> chars

lexOneToken :: CharParser Token
lexOneToken =
        lexSymbol
    <|> lexKeyword
    <|> lexIdentifier

lexer :: CharParser [ Token ]
lexer = many lexOneToken

runTokenLexer :: T.Text -> Either ParseError [Token]
runTokenLexer source = parse lexer "runTokenLexer" $ T.unpack source

unsafeRunTokenLexer :: T.Text -> [ Token ]
unsafeRunTokenLexer source = case runTokenLexer source of
  Left parseError -> error $ show parseError
  Right tokens -> tokens
