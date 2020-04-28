package dev.posco.hiona

import cats.effect.IO
import fs2.{Chunk, Pull, Stream}
import cats.collections.Heap

import dev.posco.hiona.Engine.Emitter

import cats.implicits._

object Fs2Tools {
  def streamFromFeeder(f: Feeder, batchSize: Int = 1000): Stream[IO, Point] = {
    val batch = f.nextBatch(batchSize)

    lazy val loop: Pull[IO, Point, Unit] =
      Pull
        .eval(batch)
        .flatMap {
          case Nil    => Pull.done
          case points => Pull.output(Chunk.seq(points)).flatMap(_ => loop)
        }

    loop.stream
  }

  def run[A](inputs: Stream[IO, Point], emit: Emitter[A]): Stream[IO, A] = {
    def feed(inputs: Stream[IO, Point], seq: Long): Pull[IO, A, Unit] =
      inputs.pull.uncons
        .flatMap {
          case Some((points, rest)) =>
            val batch = emit.feedAll(points.toList, seq)
            for {
              (as, s1) <- Pull.eval(batch)
              _ <- Pull.output(Chunk.seq(as))
              next <- feed(rest, s1)
            } yield next
          case None =>
            Pull.done
        }

    feed(inputs, 0L).stream
  }

  def sortMerge[F[_], A: Ordering](
      streams: List[Stream[F, A]]
  ): Stream[F, A] = {
    implicit val ord: cats.Order[Stream.StepLeg[F, A]] =
      new cats.Order[Stream.StepLeg[F, A]] {
        val ordA = implicitly[Ordering[A]]

        def compare(
            left: Stream.StepLeg[F, A],
            right: Stream.StepLeg[F, A]
        ): Int =
          if (left.head.isEmpty) {
            // prefer to step so we don't skip items
            if (right.head.isEmpty) 0 else -1
          } else if (right.head.isEmpty) {
            // we need to step so we don't misorder items
            1
          } else {
            // neither are empty just compare the head
            ordA.compare(left.head(0), right.head(0))
          }
      }

    def finish(finalLeg: Stream.StepLeg[F, A]): Pull[F, A, Unit] =
      for {
        _ <- Pull.output(finalLeg.head)
        _ <- finalLeg.stream.pull.echo
      } yield ()

    // invariant: the heap has at least two items
    def go(heap: Heap[Stream.StepLeg[F, A]]): Pull[F, A, Unit] =
      heap.pop match {
        case Some((sl, rest)) =>
          if (sl.head.nonEmpty) {
            for {
              _ <- Pull.output1(sl.head(0))
              nextSl = sl.setHead(sl.head.drop(1))
              nextHeap = rest.add(nextSl)
              // note: nextHeap.size == heap.size
              _ <- go(nextHeap)
            } yield ()
          } else {
            // this chunk is done
            sl.stepLeg
              .flatMap {
                case Some(nextSl) =>
                  val nextHeap = rest.add(nextSl)
                  // note: nextHeap.size == heap.size
                  go(nextHeap)
                case None =>
                  // this leg is exhausted
                  if (rest.size == 1) {
                    // if there is only one stream left, just
                    // emit everything from this stream without
                    // pulling one at a time

                    finish(rest.minimumOption.get)
                  } else {
                    // rest has size heap.size - 1 but > 1
                    go(rest)
                  }
              }
          }

        case None =>
          sys.error("invariant violation, expected at least 2 on each call")
      }

    streams match {
      case Nil       => Stream.empty
      case h :: Nil  => h
      case twoOrMore =>
        // we have at least two streams, but some may be empty
        twoOrMore
          .traverse(_.pull.stepLeg)
          .map(_.flatten)
          .flatMap {
            case Nil        => Pull.done
            case one :: Nil => finish(one)
            case twoOrMore  =>
              // maintain the invariant that there are at least two items in the heap
              val heap = Heap.fromIterable(twoOrMore)
              go(heap)
          }
          .stream
    }
  }
}
