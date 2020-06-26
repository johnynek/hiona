package dev.posco.hiona.aws

import cats.data.NonEmptyList
import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp, Resource}
import com.amazonaws.services.ecs.{model, AmazonECS, AmazonECSClientBuilder}
import com.amazonaws.services.ecs.model.{
  AwsVpcConfiguration,
  ContainerOverride,
  KeyValuePair,
  NetworkConfiguration,
  RunTaskRequest,
  TaskOverride
}
import com.monovore.decline.{Command, Opts}

import LambdaDeploy.Vpc

import scala.jdk.CollectionConverters._
import cats.implicits._

final class ECSDeploy(ecs: AmazonECS, blocker: Blocker)(implicit
    contextShift: ContextShift[IO]
) {
  import ECSDeploy._

  private def block[A](a: => A): IO[A] =
    blocker.blockOn(IO(a))

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

    block(ecs.runTask(req))
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
    val resp = block(
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
                  (ECSDeploy.awsEcs, Blocker[IO])
                    .mapN(new ECSDeploy(_, _))
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
