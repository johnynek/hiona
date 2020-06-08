package dev.posco.hiona.aws

import cats.effect.IO
import io.circe.Json
import com.amazonaws.services.lambda.runtime.Context

import io.circe.syntax._
import cats.implicits._

class ToyLambda extends PureLambda[Unit, Json, Json] {
  def setup = IO.unit

  def copt(o1: Option[Json], o2: Option[Json]): Option[Json] =
    (o1, o2) match {
      case (Some(j1), Some(j2)) => Some(combine(j1, j2))
      case _                    => o1.orElse(o2)
    }

  def combine(j1: Json, j2: Json): Json =
    if (j1.isNull) Json.Null
    else if (j1.isBoolean)
      (j1.asBoolean, j2.asBoolean)
        .mapN((a, b) => Json.fromBoolean(a || b))
        .getOrElse(Json.Null)
    else if (j1.isNumber)
      (j1.asNumber.map(_.toDouble), j2.asNumber.map(_.toDouble))
        .mapN((a, b) => Json.fromDouble(a + b))
        .flatten
        .getOrElse(Json.Null)
    else if (j1.isString)
      (j1.asString, j2.asString)
        .mapN { (a, b) =>
          Json.fromString(a + b)
        }
        .getOrElse(Json.Null)
    else if (j1.isArray)
      (j1.asArray, j2.asArray)
        .mapN((a, b) => Json.fromValues(a ++ b))
        .getOrElse(Json.Null)
    else if (j1.isObject)
      (j1.asObject, j2.asObject)
        .mapN { (a, b) =>
          val allKeys = a.keys.toSet | b.keys.toSet
          Json.obj(allKeys.toList.sorted.flatMap { k =>
            copt(a(k), b(k)).toList.map((k, _))
          }: _*)
        }
        .getOrElse(Json.Null)
    else Json.Null

  def run(in: IO[Json], ctx: Unit, awsCtx: Context): IO[Json] =
    in.flatMap(j => IO.fromEither(j.as[List[Json]]))
      .flatMap {
        case Nil => IO.pure(Json.Null)
        case fn :: args =>
          fn.as[String] match {
            case Right("gettime") =>
              IO.pure(System.currentTimeMillis().toString.asJson)
            case Right("combine") =>
              IO.pure(args match {
                case Nil    => Json.Null
                case h :: t => t.foldLeft(h)(combine(_, _))
              })
            case Right("merge") =>
              IO(args.foldLeft(Json.obj())(_.deepMerge(_)))
            case unknown =>
              IO.raiseError(new Exception(s"unknown fn: $unknown, args: $args"))
          }
      }
}
