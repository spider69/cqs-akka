import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source


object CalculatorReadAndWriteSide  extends App {

  sealed trait Command
  case class Add(amount: Int) extends Command
  case class Multiply(amount: Int) extends Command
  case class Divide(amount: Int) extends Command


  sealed trait Event
  case class Added(id: Int, amount: Int) extends Event
  case class Multiplied(id: Int, multiplier: Int) extends Event
  case class Divided(id: Int, multiplier: Int) extends Event

  class CalculatorWrite extends PersistentActor with ActorLogging {
    var latestCalculationId = 0
    var latestCalculationResult = 0.0

    override def persistenceId: String = "simple-calculator" // best practice: make it unique

    override def receiveCommand: Receive = {
      case Add(amount) =>
        log.info(s"Receive adding for number: $amount")
        val event = Added(latestCalculationId, amount)

        persist(event)
        { e =>
          latestCalculationId += 1
          latestCalculationResult += amount

          log.info(s"Persisted $e as adding #${e.id}, for result $latestCalculationResult")
        }
      case Multiply(amount) =>
        log.info(s"Receive multiplying for number: $amount")
        val event = Multiplied(latestCalculationId, amount)

        persist(event)
        { e =>
          latestCalculationId += 1
          latestCalculationResult *= amount

          log.info(s"Persisted $e as multiplying #${e.id}, for result $latestCalculationResult")
        }
      case Divide(amount) =>
        log.info(s"Receive dividing for number: $amount")
        val event = Divided(latestCalculationId, amount)

        persist(event)
        { e =>
          latestCalculationId += 1
          latestCalculationResult /= amount

          log.info(s"Persisted $e as dividing #${e.id}, for result $latestCalculationResult")
        }
    }

    override def receiveRecover: Receive = {
      case Added(id, amount) =>

        latestCalculationId = id
        latestCalculationResult += amount

        log.info(s"Recovered invoice #$id for amount $amount, total amount: $latestCalculationResult")
      case Multiplied(id, amount) =>

        latestCalculationId = id
        latestCalculationResult *= amount

        log.info(s"Recovered invoice #$id for amount $amount, total amount: $latestCalculationResult")
      case Divided(id, amount) =>

        latestCalculationId = id
        latestCalculationResult /= amount

        log.info(s"Recovered invoice #$id for amount $amount, total amount: $latestCalculationResult")
    }

  }


  class CalculatorRead extends Actor with ActorLogging {
    import CalculatorRepository._

    initDataBase

    var (offset, latestCalculatedResult) = getLatestOffsetAndResult

    implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
    val readJournal: LeveldbReadJournal = PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
    val events: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("simple-calculator", if(offset == 0) 0 else offset, Long.MaxValue)


    override def receive: Receive = {
      case "start" =>
        events.runForeach {
          event =>
            event.event match {
              case Added(id, amount) =>
                latestCalculatedResult += amount
                updateResultAndOfsset(latestCalculatedResult, event.sequenceNr)
                log.info(s"Saved to read store invoice #$id for amount $amount, total amount: $latestCalculatedResult")
              case Multiplied(id, amount) =>
                latestCalculatedResult *= amount
                updateResultAndOfsset(latestCalculatedResult, event.sequenceNr)
                log.info(s"Saved to read store invoice #$id for amount $amount, total amount: $latestCalculatedResult")
            }
        }
      case _    =>  println("Please write start")
    }
  }


  val system = ActorSystem("PersistentActors")
  val calculatorWriteSide = system.actorOf(Props[CalculatorWrite], "simpleCalculatorWrite")

  calculatorWriteSide ! Add(1)
  calculatorWriteSide ! Multiply(3)
  calculatorWriteSide ! Divide(4)

  val calculatorReadSide = system.actorOf(Props[CalculatorRead], "simpleCalculatorRead")
  calculatorReadSide ! "start"


}

object CalculatorRepository{
  import scalikejdbc._

  def initDataBase: Unit = {
    Class.forName("org.postgresql.Driver")
    val poolSettings = ConnectionPoolSettings(initialSize = 10, maxSize = 100)

    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/demo", "docker", "docker", poolSettings)
  }

  def getLatestOffsetAndResult: (Int, Double) = {
    val entities =
      DB readOnly { session =>
        session.list("select * from public.result where id = 1;") {
          row => (row.int("write_side_offset"), row.double("calculated_value")) }
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