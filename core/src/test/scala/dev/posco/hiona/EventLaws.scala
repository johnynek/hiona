package dev.posco.hiona

import org.scalacheck.Prop

class EventLaws extends munit.ScalaCheckSuite {
  property("Event.triggersOf has the same sources") {
    Prop.forAll(GenEventFeature.genEventTW) { tw =>
      val ev: Event[tw.Type] = tw.evidence

      val triggers = Event.triggersOf(ev.map((_, ())))

      val s1 = Event.sourcesOf(ev)
      val s2 = Event.sourcesOf(triggers)

      assertEquals(s2, s1)

      Prop(s1 == s2)
    }
  }
}
