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

package dev.posco.hiona.jobs

import dev.posco.hiona._

import cats.implicits._

object Wordcount {
  case class Data(word: String, ts: Timestamp)

  val ev: Event.Source[Data] = Event.source("data", Validator.pure[Data](_.ts))

  val countFeature: Feature[String, Long] = ev.map(d => (d.word, 1L)).sum

  val totalCount: Event[(Timestamp, String, Long)] =
    ev.map(d => (d.word, ()))
      .postLookup(countFeature)
      .valueWithTime
      .map { case (w, ((_, cnt), ts)) => (ts, w, cnt) }
}

class AwsWordcount extends aws.LambdaApp0(Output.event(Wordcount.totalCount))

object CliWordcount extends App0(Output.event(Wordcount.totalCount))
