package dev.posco.hiona.aws

import cats.effect.{Async, Blocker, ContextShift}
import doobie._

import cats.implicits._

object RDSTransactor {
  case class HostPort(host: String, port: Int)
  case class DatabaseName(asString: String)

  def build[F[_]: Async](
      region: String,
      secretName: String,
      blocker: Blocker,
      hostPort: Option[HostPort] = None
  )(implicit ctx: ContextShift[F]): F[DatabaseName => Transactor[F]] =
    Secrets
      .makeClient[F](region)
      .use(client => Secrets.getJsonSecret(client, secretName))
      .flatMap { jvalue =>
        Async[F].delay {
          val HostPort(h, p) = hostPort match {
            case None =>
              HostPort(jvalue.get("host").asString, jvalue.get("port").asInt)
            case Some(hp) => hp
          }

          val uname = jvalue.get("username").asString
          val password = jvalue.get("password").asString

          (h, p, uname, password)
        }
      }
      .map {
        case (h, p, uname, password) => {
          case DatabaseName(db) =>
            Transactor.fromDriverManager[F](
              classOf[org.postgresql.Driver].getName, // driver classname
              s"jdbc:postgresql://$h:$p/$db", // connect URL (driver-specific)
              uname,
              password,
              blocker
            )
        }
      }
}
