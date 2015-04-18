module Database.Toxic.Query.Parser where

import Database.Toxic.Types
import Database.Toxic.Query.AST
import Database.Toxic.Query.Tokenizer

import Control.Applicative ((<$>), (*>), (<*))
import Control.Monad
import qualified Data.Text as T
import qualified Data.Vector as V
import Text.Parsec
import Text.Parsec.Combinator

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

case_condition :: TokenParser (Condition, Expression)
case_condition = do
  matchToken TkWhen
  condition <- expression
  matchToken TkThen
  result <- expression
  return (condition, result)

case_else :: TokenParser Expression
case_else = matchToken TkElse *> expression

case_when_expression :: TokenParser Expression
case_when_expression = do
  matchToken TkCase
  conditions <- many $ case_condition
  else_case <- optionMaybe case_else
  matchToken TkEnd
  return $ ECase (V.fromList conditions) else_case

rename_expression :: TokenParser Expression
rename_expression = do
  original <- expression
  new_name <- rename_clause
  return $ ERename original new_name

expression :: TokenParser Expression
expression =
        try(ELiteral <$> literal)
    <|> case_when_expression

rename_clause :: TokenParser T.Text
rename_clause = do
  matchToken TkRename
  new_name_token <- identifier
  case new_name_token of
    TkIdentifier new_name -> return new_name
    _ -> error $ "rename_clause: Unexpected value - " ++ show new_name_token

select_item :: TokenParser Expression
select_item =
      try (expression <* notFollowedBy rename_clause)
  <|> rename_expression

select_clause :: TokenParser (ArrayOf Expression)
select_clause = V.fromList <$> many select_item

subquery :: TokenParser Query
subquery = matchToken TkOpen *> query <* matchToken TkClose

from_clause :: TokenParser Query
from_clause = matchToken TkFrom *> (
  try product_query <|> try subquery
  )

single_query :: TokenParser Query
single_query = matchToken TkSelect *> do
  expressions <- select_clause
  source <- optionMaybe from_clause
  return $ SingleQuery { queryProject = expressions, querySource = source }

composite_query :: TokenParser Query
composite_query =
  let one_or_more = V.fromList <$>
                    sepBy1 single_query (matchToken TkUnion)
  in do
    composite <- one_or_more
    when (V.length composite == 1) $ fail "A single composite query is just a single query"
    return $ CompositeQuery QueryCombineUnion composite

product_query :: TokenParser Query
product_query = 
  let one_or_more = V.fromList <$>
                    sepBy1 subquery (matchToken TkSequence)
  in do
    subqueries <- one_or_more
    when (V.length subqueries == 1) $ fail "A single product query is just a single query"
    return $ ProductQuery { queryFactors = subqueries }

query :: TokenParser Query
query = try composite_query <|> single_query

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
