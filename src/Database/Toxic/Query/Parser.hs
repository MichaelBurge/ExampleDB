module Database.Toxic.Query.Parser where

import Database.Toxic.Types
import Database.Toxic.Query.AST
import Database.Toxic.Query.Tokenizer

import Control.Applicative ((<$>), (*>), (<*))
import qualified Data.Vector as V
import Text.Parsec

type TokenParser a = Parsec [Token] () a

matchToken :: Token -> TokenParser Token
matchToken token = tokenPrim show ignorePosition is_token
  where
    is_token x | x == token = Just token
    is_token _ = Nothing

literal :: TokenParser Literal
literal =
      (matchToken TkTrue *> return (LBool True))
  <|> (matchToken TkFalse *> return (LBool False))

expression :: TokenParser Expression
expression = ELiteral <$> literal

select_clause :: TokenParser (ArrayOf Expression)
select_clause = V.fromList <$> many expression
  
query :: TokenParser Query
query = matchToken TkSelect *> do
  expressions <- select_clause
  return $ Query { queryProject = expressions }

statement :: TokenParser Statement
statement = do
  q <- query
  matchToken TkStatementEnd
  return $ SQuery q

runTokenParser :: [Token] -> Either ParseError Statement
runTokenParser tokens = parse statement "runTokenParser" tokens

unsafeRunTokenParser :: [Token] -> Statement
unsafeRunTokenParser tokens = case runTokenParser tokens of
  Left parseError -> error $ show parseError
  Right statement -> statement

ignorePosition pos _ _ = pos
