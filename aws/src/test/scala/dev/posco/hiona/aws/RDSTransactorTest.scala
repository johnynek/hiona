/*
 * Copyright 2022 devposco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
