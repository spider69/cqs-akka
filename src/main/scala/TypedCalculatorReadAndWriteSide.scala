import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Props, _}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.scaladsl.Source
import akka_typed.TypedCalculatorWriteSide._

object akka_typed {
  trait CborSerializable

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Int) extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int) extends Command

    sealed trait Event
    case class Added(id: Int, amount: Int) extends Event
    case class Multiplied(id: Int, amount: Int) extends Event
    case class Divided(id: Int, amount: Int) extends Event

    final case class State(value: Int) extends CborSerializable {
      def add(amount: Int): State = copy(value = value + amount)
      def multiply(amount: Int): State = copy(value = value * amount)
      def divide(amount: Int): State = copy(value = value / amount)
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

    def handleCommand(persistenceId: String, state: State, command: Command, ctx: ActorContext[Command]): Effect[Event, State] =
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
    import CalculatorRepository._
    initDataBase

    implicit val materializer = system.classicSystem
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

  def main(args: Array[String]): Unit = {
    val system: ActorSystem[NotUsed] = ActorSystem(akka_typed(), "akka_typed")

    TypedCalculatorReadSide(system)
  }
}

object CalculatorRepository {
  import scalikejdbc._

  def initDataBase: Unit = {
    Class.forName("org.postgresql.Driver")
    val poolSettings = ConnectionPoolSettings(initialSize = 1, maxSize = 10)
    ConnectionPool.singleton(
      "jdbc:postgresql://localhost:5432/demo",
      "docker",
      "docker",
      poolSettings
    )
  }

  def getLatestOffsetAndResult: (Int, Double) = {
    val entities =
      DB readOnly { session =>
        session.list("select * from public.result where id = 1;") { row =>
          (row.int("write_side_offset"), row.double("calculated_value"))
        }
      }
    entities.head
  }

  def updateResultAndOfsset(calculated: Double, offset: Long): Unit = {
    using(DB(ConnectionPool.borrow())) { db =>
      db.autoClose(true)
      db.localTx {
        _.update("update public.result set calculated_value = ?, write_side_offset = ? where id = ?", calculated, offset, 1)
      }
    }
  }
}