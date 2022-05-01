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

package dev.posco.hiona

import org.scalacheck.Prop.forAll

class Fs2ToolsTests extends munit.ScalaCheckSuite {
  property("sortMerge on lists works like sort") {
    forAll { lists: List[List[Int]] =>
      val realSorted = lists.flatten.sorted

      val sortMerge = lists.map(_.sorted).map(fs2.Stream.emits(_))

      val sm = Fs2Tools.sortMerge(sortMerge)

      assertEquals(sm.compile.toList, realSorted)
    }
  }
}
