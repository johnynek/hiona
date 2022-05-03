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

import cats.effect.{Resource, Sync}
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.amazonaws.services.secretsmanager.{
  AWSSecretsManager,
  AWSSecretsManagerClientBuilder
}
import io.circe.Json
import io.circe.jawn.CirceSupportParser
import java.nio.ByteBuffer

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
  ): F[Json] =
    getSecret(client, secretName)
      .flatMap {
        case Right(str) =>
          Sync[F].fromTry(CirceSupportParser.parseFromString(str))
        case Left(bb) =>
          Sync[F].fromTry(CirceSupportParser.parseFromByteBuffer(bb))
      }
}
