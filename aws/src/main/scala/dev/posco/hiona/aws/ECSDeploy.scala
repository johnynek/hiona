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

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.amazonaws.services.ecs.model.{
  AwsVpcConfiguration,
  ContainerOverride,
  KeyValuePair,
  NetworkConfiguration,
  RunTaskRequest,
  TaskOverride
}
import com.amazonaws.services.ecs.{AmazonECS, AmazonECSClientBuilder, model}
import com.monovore.decline.{Command, Opts}
import scala.jdk.CollectionConverters._

import cats.implicits._

import LambdaDeploy.Vpc

final class ECSDeploy(ecs: AmazonECS) {
  import ECSDeploy._

  def run(
      cluster: Cluster,
      task: TaskDef,
      vpc: Vpc,
      image: ImageName,
      cmd: NonEmptyList[String],
      env: Map[String, String]
  ): IO[TaskId] = {

    val req = (new RunTaskRequest)
      .withNetworkConfiguration(
        (new NetworkConfiguration)
          .withAwsvpcConfiguration(toAws(vpc))
      )
      .withOverrides(
        (new TaskOverride)
          .withContainerOverrides {
            val o = new ContainerOverride
            Option(env)
              .filter(_.nonEmpty)
              .fold(o) { env =>
                o.withEnvironment(toKVPairs(env): _*)
              }
              .withCommand(cmd.toList: _*)
              .withName(image.asString)
          }
      )
      .withTaskDefinition(task.asString)
      .withCluster(cluster.asString)
      .withCount(1)
      .withEnableECSManagedTags(true)
      .withPlatformVersion("1.4.0")
      .withLaunchType("FARGATE")

    IO.blocking(ecs.runTask(req))
      .flatMap { resp =>
        resp.getTasks.iterator.asScala.toList match {
          case t0 :: Nil =>
            IO.pure(TaskId(t0.getTaskArn))
          case other =>
            /*
             * In principle, a task definition can have many
             * internal tasks, but this code currently supports
             * running a task definition with a single container
             * task
             */
            IO.raiseError(
              new IllegalStateException(
                s"expected just one task, found: $other in $req"
              )
            )
        }
      }
  }

  def getImages(taskDef: TaskDef): IO[NonEmptyList[ImageName]] = {
    val resp = IO.blocking(
      ecs.describeTaskDefinition(
        new model.DescribeTaskDefinitionRequest()
          .withTaskDefinition(taskDef.asString)
      )
    )

    resp.flatMap { r =>
      r.getTaskDefinition.getContainerDefinitions.asScala.toList match {
        case Nil =>
          /*
           * We don't expect zero containers, but the response
           * could have zero in principle (the types don't forbid it).
           * However, we return a non-empty list which does forbid
           * having zero items
           */
          IO.raiseError(
            new IllegalStateException(
              s"expected at least one container: $r for $taskDef"
            )
          )
        case h :: tail =>
          IO.pure(NonEmptyList(h, tail).map(cd => ImageName(cd.getName)))
      }
    }
  }

  def getSingleImage(taskDef: TaskDef): IO[ImageName] =
    getImages(taskDef)
      .flatMap {
        case NonEmptyList(i, Nil) => IO.pure(i)
        case moreThanOne          =>
          // If there is more than one image in the task, return an error
          IO.raiseError(
            new IllegalStateException(
              s"expected just one image for: $taskDef, found: $moreThanOne"
            )
          )
      }
}

object ECSDeploy {
  case class TaskDef(asString: String)
  case class TaskId(asString: String)
  case class Cluster(asString: String)
  case class ImageName(asString: String)

  def toAws(vpc: LambdaDeploy.Vpc): AwsVpcConfiguration =
    new AwsVpcConfiguration()
      .withSubnets(vpc.subnets.toList.sorted: _*)
      .withSecurityGroups(vpc.securityGroups.toList.sorted: _*)

  def toKVPairs(m: Map[String, String]): List[KeyValuePair] =
    m.iterator.map {
      case (key, value) =>
        (new KeyValuePair).withName(key).withValue(value)
    }.toList

  val awsEcs: Resource[IO, AmazonECS] =
    Resource.make(IO(AmazonECSClientBuilder.defaultClient())) { ecs =>
      IO(ecs.shutdown())
    }
}

object ECSDeployApp extends IOApp {
  import ECSDeploy._

  val runCmd: Opts[List[String] => IO[ExitCode]] = {

    def str[A](nm: String, help: String)(fn: String => A): Opts[A] =
      Opts.option[String](nm, help).map(fn)

    val defaultVpc: Opts[Vpc] =
      Vpc.vpcOpts.map {
        case Some(vpc) => vpc
        case None =>
          Vpc(
            subnets = Set("subnet-108a2467"),
            securityGroups = Set("sg-2defd948")
          )
      }

    val args: Opts[
      (
          Cluster,
          TaskDef,
          Vpc,
          IO[Map[String, String]]
      )
    ] =
      (
        str("cluster", "the cluster to run on")(Cluster(_)).withDefault(
          Cluster("pm-fargate-oregon")
        ),
        str("taskdef", "the arn of the task definition")(TaskDef(_)),
        defaultVpc,
        LambdaDeploy.Payload.optPayload("env").orNone.map {
          case None => IO.pure(Map.empty[String, Nothing])
          case Some(io) =>
            for {
              json <- io.toJson
              m <- IO.fromEither(json.as[Map[String, String]])
            } yield m
        }
      ).tupled

    args.map {
      case (c, task, v, readEnvJson) =>
        (args: List[String]) =>
          NonEmptyList.fromList(args) match {
            case None =>
              IO.raiseError(
                new IllegalArgumentException(
                  "expected at least one argument to run the container"
                )
              )
            case Some(args) =>
              readEnvJson
                .flatMap { env =>
                  ECSDeploy.awsEcs
                    .map(new ECSDeploy(_))
                    .use { ecs =>
                      for {
                        image <- ecs.getSingleImage(task)
                        tid <- ecs.run(c, task, v, image, args, env)
                        _ <- IO(println(tid.asString))
                      } yield ExitCode.Success
                    }
                }
          }
    }
  }

  val cmd: Command[List[String] => IO[ExitCode]] =
    Command("ecsdeploy", "tool to run tasks on ECS") {
      Opts.subcommands(
        Command(
          "run",
          "run a registered task definition with given set of arguments"
        )(runCmd)
      )
    }

  /**
    * use -- to delimit where the args for the current process end
    * pass the first block to this argument parting, and the rest into the container
    */
  def splitArgs(args: List[String]): (List[String], List[String]) = {
    @annotation.tailrec
    def loop(
        args: List[String],
        revHead: List[String]
    ): (List[String], List[String]) =
      args match {
        case Nil          => (revHead.reverse, Nil)
        case "--" :: tail => (revHead.reverse, tail)
        case head :: tail => loop(tail, head :: revHead)
      }

    loop(args, Nil)
  }

  def run(args: List[String]): IO[ExitCode] = {
    val (theseArgs, nextArgs) = splitArgs(args)
    cmd.parse(theseArgs) match {
      case Right(io) => io(nextArgs)
      case Left(err) =>
        for {
          _ <- IO(System.err.println(err))
        } yield ExitCode.Error
    }
  }
}
