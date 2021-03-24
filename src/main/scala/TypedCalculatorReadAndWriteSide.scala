import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Props, _}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka_typed.TypedCalculatorWriteSide.{Add, Added, Command, Divide, Divided, Multiplied, Multiply}
import spray.json.{DefaultJsonProtocol, enrichAny, _}

import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

case class Action(value: Int, name: String)

trait ActionJsonProtocol extends DefaultJsonProtocol {
  implicit val personJson = jsonFormat2(Action)
}

object akka_typed extends ActionJsonProtocol
{
  trait CborSerializable

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Int)      extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int)   extends Command

    sealed trait Event
    case class Added(id: Int, amount: Int)      extends Event
    case class Multiplied(id: Int, amount: Int) extends Event
    case class Divided(id: Int, amount: Int)    extends Event

    final case class State(value: Int) extends CborSerializable {
      def add(amount: Int): State      = copy(value = value + amount)
      def multiply(amount: Int): State = copy(value = value * amount)
      def divide(amount: Int): State   = copy(value = value / amount)
    }

    object State {
      val empty = State(0)
    }

    def apply(): Behavior[Command] =
      Behaviors.setup { ctx =>
      /**
       * Акторы с состоянием, которое персистется в журнал и может быть восстановлено, при сбоях в JVM, ручном рестарте или при миграции на кластере
       * Сохраняются только события, не состояние. Хотя иногда можно сохранить состояние.
       * События персистятся путем добавления в хранилище (НЕ ИЗМЕНЕНИЯ), оптимизированное под запись, что повышает скорость записи
       *
       * Восстановление актора происходит за счет воспроизведения сохраненных событий.
       *
       *
       *
       * Узнаете:
       * 1) Что делать если событий очень много?
       * 2) Настроим хранилище с кассандрой
       * 3)
       * */


        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

    def handleCommand(
        persistenceId: String,
        state: State,
        command: Command,
        ctx: ActorContext[Command]
    ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"Receive adding for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"Receive multiplying for number: $amount and state is ${state.value}")
          Effect
            .persist(Added(persistenceId.toInt, amount))
            .thenRun { newState =>
              ctx.log.info(s"The state result is ${newState.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"Receive dividing for number: $amount and state is ${state.value}")
          Effect
            .persist(Added(persistenceId.toInt, amount))
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.divide(amount)
      }
  }

  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]) {
    import dao.CalculatorRepository._
    initDataBase

    implicit val materializer            = system.classicSystem
    var (offset, latestCalculatedResult) = getLatestOffsetAndResult
    val startOffset: Int                 = if (offset == 0) 0 else offset

    val readJournal: LeveldbReadJournal =
      PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)

    val source: Source[EventEnvelope, NotUsed] = readJournal
      .eventsByPersistenceId("001", startOffset, Long.MaxValue)

    source.runForeach { event =>
      event.event match {
        case Added(_, amount) =>
          latestCalculatedResult += amount
          updateResultAndOfsset(latestCalculatedResult, event.sequenceNr)
          println(s"! TEst test: $latestCalculatedResult")
        case Multiplied(_, amount) =>
          latestCalculatedResult += amount
          updateResultAndOfsset(latestCalculatedResult, event.sequenceNr)
          println(s"! TEst test: $latestCalculatedResult")
        case Divided(_, amount) =>
          latestCalculatedResult += amount
          updateResultAndOfsset(latestCalculatedResult, event.sequenceNr)
          println(s"! TEst test: $latestCalculatedResult")
      }
    }
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calculato", Props.empty)

      writeActorRef ! Add(10)
      writeActorRef ! Multiply(2)
      writeActorRef ! Divide(5)

      Behaviors.same
    }

  def execute(comm: Command): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calculato", Props.empty)

      writeActorRef ! comm

      Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")

    execute(Add(1000))

    TypedCalculatorReadSide(system)

    implicit val executionContext = system.executionContext


    var actions = List(
      Action(1, "Add"),
      Action(2, "Multiply"),
      Action(3, "Divide")
    )

    val personServerRoute =
      pathPrefix("api" / "action") {
        get {
          pathEndOrSingleSlash {
            complete(
              HttpEntity(
                ContentTypes.`application/json`,

                actions.toJson.prettyPrint
              )
            )
          }
        } ~
          (post & pathEndOrSingleSlash & extractRequest & extractLog) { (request, log) =>
            val entity             = request.entity
            val strictEntityFuture = entity.toStrict(2 seconds)
            val actionFuture       = strictEntityFuture.map(_.data.utf8String.parseJson.convertTo[Action])

            onComplete(actionFuture) {
              case Success(action) =>
                log.info(s"Got action: $action")

                actions = actions :+ action
                complete(StatusCodes.OK)
              case Failure(ex) =>
                failWith(ex)
            }
          }
      }

    val bindingFuture = Http().newServerAt("localhost", 8080).bind(personServerRoute)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind())                 // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}