module Database.Toxic.Query.Parser where

import Database.Toxic.Types
import Database.Toxic.Query.AST
import Database.Toxic.Query.Tokenizer

import Control.Applicative ((<$>), (*>), (<*))
import qualified Data.Text as T
import qualified Data.Vector as V
import Text.Parsec

type TokenParser a = Parsec [Token] () a

matchToken :: Token -> TokenParser Token
matchToken token = tokenPrim show ignorePosition is_token
  where
    is_token x | x == token = Just token
    is_token _ = Nothing

identifier :: TokenParser Token
identifier = tokenPrim show ignorePosition is_token
  where
    is_token token@(TkIdentifier x) = Just token
    is_token _ = Nothing

literal :: TokenParser Literal
literal =
      (matchToken TkTrue *> return (LBool True))
  <|> (matchToken TkFalse *> return (LBool False))

rename_expression :: TokenParser Expression
rename_expression = do
  original <- expression
  new_name <- rename_clause
  return $ ERename original new_name

expression :: TokenParser Expression
expression =
      ELiteral <$> literal

rename_clause :: TokenParser T.Text
rename_clause = do
  matchToken TkRename
  new_name_token <- identifier
  case new_name_token of
    TkIdentifier new_name -> return new_name

select_item :: TokenParser Expression
select_item =
      try (expression <* notFollowedBy rename_clause)
  <|> rename_expression

select_clause :: TokenParser (ArrayOf Expression)
select_clause = V.fromList <$> many select_item
  
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
