{-# LANGUAGE OverloadedStrings #-}

module Database.Toxic.Query.Parser where

import Database.Toxic.Types
import Database.Toxic.Query.AST

import Control.Applicative ((<$>), (*>), (<*), (<*>))
import Control.Monad
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Vector as V
import Text.Parsec
import Text.Parsec.Combinator
import Text.Parsec.Language
import qualified Text.Parsec.Token as P

type CharParser a = Parsec String () a

sqlLanguageDef = P.LanguageDef {
  P.commentStart = "",
  P.commentEnd = "",
  P.commentLine = "--",
  P.nestedComments = False,
  P.identStart = letter <|> char '_',
  P.identLetter = alphaNum <|> char '_',
  P.opStart = oneOf "",
  P.opLetter = oneOf "",
  P.reservedNames = [],
  P.reservedOpNames = [],
  P.caseSensitive = False
  }

lexer = P.makeTokenParser sqlLanguageDef

integer :: CharParser Literal
integer = LInt <$> P.integer lexer

identifier :: CharParser T.Text
identifier =
  let nondigit = letter <|> char '_'
      allowedChar = nondigit <|> digit
      chars = (:) <$> nondigit <*> many allowedChar
  in T.pack <$> chars <* spaces

keyword :: String -> CharParser ()
keyword text = try ( string text *> spaces *> return ())

union_all :: CharParser ()
union_all = keyword "union" *> keyword "all"

literal :: CharParser Literal
literal =
  let true = keyword "true" *> return (LBool True)
      false = keyword "false" *> return (LBool False)
      null = keyword "null" *> return LNull
  in true <|> false <|> null <|> integer

case_condition :: CharParser (Condition, Expression)
case_condition = do
  keyword "when"
  condition <- expression
  keyword "then"
  result <- expression
  return (condition, result)

case_else :: CharParser Expression
case_else = keyword "else" *> expression

case_when_expression :: CharParser Expression
case_when_expression = do
  keyword "case"
  conditions <- many $ case_condition
  else_case <- optionMaybe case_else
  keyword "end"
  return $ ECase (V.fromList conditions) else_case
  
variable :: CharParser Expression
variable = EVariable <$> identifier

expression :: CharParser Expression
expression =
        try(ELiteral <$> literal)
    <|> case_when_expression
    <|> try function
    <|> variable

rename_clause :: CharParser T.Text
rename_clause = keyword "as" *> identifier

select_item :: CharParser Expression
select_item = do 
  x <- expression
  rename <- optionMaybe rename_clause
  return $ case rename of
    Just name -> ERename x name
    Nothing -> x

select_clause :: CharParser (ArrayOf Expression)
select_clause = do
  keyword "select"
  V.fromList <$> sepBy1 select_item (keyword ",")

group_by_clause :: CharParser (ArrayOf Expression)
group_by_clause = do
  keyword "group"
  keyword "by"
  expressions <- V.fromList <$> sepBy1 expression (keyword ",")
  return expressions

order_by_clause :: CharParser (ArrayOf Expression)
order_by_clause = do
  keyword "order by"
  expressions <- V.fromList <$> sepBy1 expression (keyword ",")
  return expressions

subquery :: CharParser Query
subquery = keyword "(" *> query <* keyword ")"

function :: CharParser Expression
function = do
  name <- identifier
  keyword "("
  argument <- expression
  keyword ")"
  case name of
    "bool_or" -> return $ EAggregate QAggBoolOr argument
    "sum" -> return $ EAggregate QAggSum argument
    _ -> fail $ T.unpack $ "Unknown function " <> name
  

from_clause :: CharParser (Maybe Query)
from_clause =
  let real_from_clause = do
        try $ keyword "from"
        product_query
  in (Just <$> real_from_clause) <|>
     return Nothing

single_query :: CharParser Query
single_query = do
  expressions <- select_clause
  source <- from_clause
  groupBy <- optionMaybe group_by_clause
  orderBy <- optionMaybe order_by_clause
  return $ SingleQuery {
    queryGroupBy = groupBy,
    queryProject = expressions,
    querySource = source,
    queryOrderBy = orderBy
    }
    
composite_query :: CharParser Query
composite_query =
  let one_or_more = V.fromList <$>
                    sepBy1 single_query union_all
  in do
    composite <- one_or_more
    return $ if V.length composite == 1
             then V.head composite
             else SumQuery QuerySumUnionAll composite

product_query :: CharParser Query
product_query = 
  let one_or_more = V.fromList <$>
                    sepBy1 subquery (keyword ",")
  in do
    subqueries <- one_or_more
    return $ if V.length subqueries == 1
             then V.head subqueries
             else ProductQuery { queryFactors = subqueries }

query :: CharParser Query
query = composite_query <?> "Expected a query or union of queries"

statement :: CharParser Statement
statement = do
  q <- query
  char ';'
  return $ SQuery q

runQueryParser :: T.Text -> Either ParseError Statement
runQueryParser text = parse statement "runTokenParser" $ T.unpack text

unsafeRunQueryParser :: T.Text -> Statement
unsafeRunQueryParser text = case runQueryParser text of
  Left parseError -> error $ show parseError
  Right statement -> statement
