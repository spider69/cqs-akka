package write_side

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

object PersistenceActor extends App{

    // COMMANDS
  case class Add(amount: Int)
  case class Multiply(multiplyer: Int)

  // EVENTS
  case class Added(id: Int, amount: Int)

  class Calculator extends PersistentActor with ActorLogging {

    var latestCalculationId = 0
    var latestCalculationResult = 0

    override def persistenceId: String = "simple-accountant" // best practice: make it unique

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

    }

    override def receiveRecover: Receive = {

      case Added(id, amount) =>
        latestCalculationId = id
        latestCalculationResult += amount
        log.info(s"Recovered invoice #$id for amount $amount, total amount: $latestCalculationResult")
    }

  }

  val system = ActorSystem("PersistentActors")
  val calculator = system.actorOf(Props[Calculator], "simpleCalculator")

  calculator ! Add(1)
  calculator ! Multiply(4)

}
