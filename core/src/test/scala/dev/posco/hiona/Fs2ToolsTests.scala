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
