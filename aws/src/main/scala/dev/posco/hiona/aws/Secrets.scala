package dev.posco.hiona.aws

import com.amazonaws.services.secretsmanager.{
  AWSSecretsManager,
  AWSSecretsManagerClientBuilder
}
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import cats.effect.{Resource, Sync}
import java.nio.ByteBuffer
import org.typelevel.jawn.{ast => jawn}

import cats.implicits._

object Secrets {
  def makeClient[F[_]: Sync](region: String): Resource[F, AWSSecretsManager] =
    Resource.make {
      // Create a Secrets Manager client
      Sync[F].delay(
        AWSSecretsManagerClientBuilder
          .standard()
          .withRegion(region)
          .build()
      )

    }(asm => Sync[F].delay(asm.shutdown()))

  def getSecret[F[_]: Sync](
      client: AWSSecretsManager,
      secretName: String
  ): F[Either[ByteBuffer, String]] = {

    val getSecretValueRequest =
      new GetSecretValueRequest().withSecretId(secretName);

    Sync[F]
      .delay(client.getSecretValue(getSecretValueRequest))
      .map { getSecretValueResult =>
        getSecretValueResult.getSecretString() match {
          case null =>
            Left(getSecretValueResult.getSecretBinary())
          case notNull => Right(notNull)
        }
      }
  }

  def getJsonSecret[F[_]: Sync](
      client: AWSSecretsManager,
      secretName: String
  ): F[jawn.JValue] =
    getSecret(client, secretName)
      .flatMap {
        case Right(str) => Sync[F].fromTry(jawn.JParser.parseFromString(str))
        case Left(bb)   => Sync[F].fromTry(jawn.JParser.parseFromByteBuffer(bb))
      }
}
