import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.{Offset, PersistenceQuery}
import akka.stream.ActorMaterializer


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


  class WordCountActor extends Actor {
    // internal data
    var totalWords = 0

    // behavior
    def receive: Receive = {
      case message: String =>
        println(s"[word counter] I have received: $message")
        totalWords += message.split(" ").length
      case msg => println(s"[word counter] I cannot understand ${msg.toString}")
    }
  }

  class CalculatorRead extends Actor {
    override def receive: Receive = {
      case "start" =>  ???


    }
  }

  val system = ActorSystem("PersistentActors")
  val calculator = system.actorOf(Props[CalculatorWrite], "simpleCalculator")

//  calculator ! Add(1)
//  calculator ! Multiply(3)
//  calculator ! Divide(4)

  CalculatorRepository.initDataBase

  println(CalculatorRepository.getLatestOffset)


  // Read side
  implicit val materializer = ActorMaterializer()(system)

  val readJournal = PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)

  val offset = CalculatorRepository.getLatestOffset

  val events = readJournal.eventsByPersistenceId("simple-calculator", if(offset == 0) 0 else offset, Long.MaxValue)

  var latestWriteCalculationResult = 0.0

  events.runForeach {
    event =>
      event.event match {
        case Added(id, amount) =>
          latestWriteCalculationResult += amount

          CalculatorRepository.updateResultAndOfsset(latestWriteCalculationResult, event.sequenceNr)
          println(s"Saved to read store invoice #$id for amount $amount, total amount: $latestWriteCalculationResult")

//          log.info(s"Saved to read store invoice #$id for amount $amount, total amount: $latestWriteCalculationResult")
        case Multiplied(id, amount) =>
          latestWriteCalculationResult *= amount

          CalculatorRepository.updateResultAndOfsset(latestWriteCalculationResult, event.sequenceNr)
          println(s"Saved to read store invoice #$id for amount $amount, total amount: $latestWriteCalculationResult")

//          log.info(s"Saved to read store invoice #$id for amount $amount, total amount: $latestWriteCalculationResult")
        case Divided(id, amount) =>
          latestWriteCalculationResult /= amount

          CalculatorRepository.updateResultAndOfsset(latestWriteCalculationResult, event.sequenceNr)
          println(s"Saved to read store invoice #$id for amount $amount, total amount: $latestWriteCalculationResult")

        //          log.info(s"Saved to read store invoice #$id for amount $amount, total amount: $latestWriteCalculationResult")
      }
  }

}

object CalculatorRepository{
  import scalikejdbc._

  def initDataBase: Unit = {
    Class.forName("org.postgresql.Driver")
    val poolSettings = ConnectionPoolSettings(initialSize = 10, maxSize = 100)

    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/demo", "docker", "docker", poolSettings)
  }

  def getLatestOffset: Int = {
    val entities =
      DB readOnly { session =>
        session.list("select * from public.result where id = 1;") { row => row.int("write_side_offset") }
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