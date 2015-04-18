import Test.Framework (defaultMain)

import Tests.Database.Toxic.Query.Interpreter (interpreterTests)
import Tests.Database.Toxic.Query.Parser (parserTests)
import Tests.Database.Toxic.Query.Tokenizer (tokenizerTests)

import Tests.ExampleQueries (exampleQueriesTests)

main = defaultMain tests
tests =
  [
    interpreterTests,
    parserTests,
    tokenizerTests,
    exampleQueriesTests
  ]