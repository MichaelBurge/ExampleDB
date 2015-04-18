import Test.Framework (defaultMain)

import Tests.Database.Toxic.Query.Interpreter (interpreterTests)
import Tests.Database.Toxic.Query.Tokenizer (tokenizerTests)

main = defaultMain tests
tests =
  [
    interpreterTests,
    tokenizerTests
  ]
