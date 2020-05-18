package dev.posco.hiona.aws

import io.circe.parser.decode

import RDSTransactor.HostPort

class RDSTransactorTest extends munit.ScalaCheckSuite {
  test("we can parse any json with the right keys") {
    def example(hp: HostPort, str: String) =
      assertEquals(decode[HostPort](str), Right(hp))

    example(HostPort("foo", 42), "{\"host\": \"foo\", \"port\": 42}")
    example(
      HostPort("foo", 41),
      "{\"host\": \"foo\", \"port\": 41, \"baz\": 44}"
    )
  }
}
