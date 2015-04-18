import Test.Framework (defaultMain)

import Tests.Database.Toxic.Query.Interpreter (interpreterTests)

main = defaultMain tests
tests =
  [
    interpreterTests
  ]
