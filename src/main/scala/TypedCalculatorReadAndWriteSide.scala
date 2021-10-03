import akka.NotUsed
import akka.actor.typed._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.ClosedShape
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka_typed.CalculatorRepository._
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps



case class Action(value: Int, name: String)


object akka_typed
{
  trait CborSerializable

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Int)      extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int)   extends Command

    sealed trait Event
    case class Added(id: Int, amount: Double)      extends Event
    case class Multiplied(id: Int, amount: Double) extends Event
    case class Divided(id: Int, amount: Double)    extends Event

    final case class State(value: Double) extends CborSerializable {
      def add(amount: Double): State      = copy(value = value + amount)
      def multiply(amount: Double): State = copy(value = value * amount)
      def divide(amount: Double): State   = copy(value = value / amount)
    }

    object State {
      val empty = State(0)
    }

    def apply(): Behavior[Command] =
      Behaviors.setup { ctx =>
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
            .persist(Multiplied(persistenceId.toInt, amount))
            .thenRun { newState =>
              ctx.log.info(s"The state result is ${newState.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"Receive dividing for number: $amount and state is ${state.value}")
          Effect
            .persist(Divided(persistenceId.toInt, amount))
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

  import TypedCalculatorWriteSide._

  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]) {
    implicit val session = createSession()
    system.classicSystem.registerOnTermination(() => session.close())

    implicit val materializer            = system.classicSystem
    var (latestCalculatedResult, offset) = getLatestOffsetAndResult
    val startOffset: Long                 = if (offset == 1) 1 else offset + 1

//    val readJournal: LeveldbReadJournal =
    val readJournal: CassandraReadJournal =
      PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)


    /**
     * В read side приложения с архитектурой CQRS (объект TypedCalculatorReadSide в TypedCalculatorReadAndWriteSide.scala)
     * необходимо разделить бизнес логику и запись в целевой получатель, т.е.
     * 1) Persistence Query должно находиться в Source
     * 2) Обновление состояния необходимо переместить в отдельный от записи в БД флоу
     * 3) ! Задание со звездочкой: вместо CalculatorRepository создать Sink c любой БД (например Postgres из docker-compose файла).
     * Для последнего задания пригодится документация - https://doc.akka.io/docs/alpakka/current/slick.html#using-a-slick-flow-or-sink
     * Результат выполненного д.з. необходимо оформить либо на github gist либо PR к текущему репозиторию.
     *
     * */

    val source: Source[EventEnvelope, NotUsed] = readJournal
      .eventsByPersistenceId("001", startOffset, Long.MaxValue)

    def updateState(event: Any, seqNum: Long): Result = {
      val newState = event match {
        case Added(_, amount) =>
          latestCalculatedResult + amount
        case Multiplied(_, amount) =>
          latestCalculatedResult * amount
        case Divided(_, amount) =>
          latestCalculatedResult / amount
      }
      Result(newState, seqNum)
    }

    val graph = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val input = builder.add(source)
      val stateUpdater = builder.add(Flow[EventEnvelope].map(e => updateState(e.event, e.sequenceNr)))
      val localSaveOutput = builder.add(Sink.foreach[Result] { r =>
          latestCalculatedResult = r.state
          println("SAVE TO LOCAL")
      })
      val dbSaveOutput = builder.add(
        Slick.sink[Result](r => updateOffsetAndResult(r))
      )
      val broadcast = builder.add(Broadcast[Result](2))

      input ~> stateUpdater
               stateUpdater ~> broadcast
                               broadcast.out(0) ~> localSaveOutput
                               broadcast.out(1) ~> dbSaveOutput

      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  object CalculatorRepository {
    def createSession(): SlickSession = {
      val db = Database.forConfig("slick-postgres.db")
      val profile = slick.jdbc.PostgresProfile
      SlickSession.forDbAndProfile(db, profile)
    }

    def getLatestOffsetAndResult(implicit session: SlickSession): (Double, Long) = {
      import session.profile.api._
      val query = sql"select calculated_value, write_side_offset from public.result where id = 1;"
        .as[(Double, Long)]
        .headOption
      val future = session.db.run(query)
      Await.result(future, 1 minute).getOrElse(throw new Exception("Result offset does not exist"))
    }

    case class Result(state: Double, offset: Long)

    def updateOffsetAndResult(result: Result)(implicit session: SlickSession) = {
      import session.profile.api._
      sqlu"update public.result set calculated_value = ${result.state}, write_side_offset = ${result.offset} where id = 1"
    }

  }


  def apply(): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calculato", Props.empty)

//      writeActorRef ! Add(10)
//      writeActorRef ! Multiply(2)
//      writeActorRef ! Divide(5)

      // 0 + 10 = 10
      // 10 * 2 = 20
      // 20 / 5 = 4

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

    TypedCalculatorReadSide(system)

    implicit val executionContext = system.executionContext
  }
}