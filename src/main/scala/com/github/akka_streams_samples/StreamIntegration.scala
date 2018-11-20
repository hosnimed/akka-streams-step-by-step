package com.github.akka_streams_samples

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Kill, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl._
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


object StreamIntegration extends App {


  implicit val system = ActorSystem("stream-integration")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global
  implicit val timeOut = Timeout(5.seconds)
  /**
    * Sample Actor
    */
  class SampleActor extends Actor with ActorLogging {
    override def receive: Receive = {
//      case even:Int if even % 2 == 0 => sender() ! even
      case n:Int if n % 2 == 0 => {
        log.info(s"${self} received $n - processing ...")
        val d = n * 2
        sender() ! d
      }
      case msg => {
        log.info(s"${self} received $msg ")
//        sender() ! Failure (new Throwable("Not even number!"))
//        self ! Kill
        sender() forward(msg)
      }
    }

  }
  val sampleActorRef: ActorRef = system.actorOf(Props[SampleActor], "sample-actor")
/*
  Source(0 to 10)
    .ask[Int](1)(sampleActorRef)
    .toMat(Sink.ignore)(Keep.right) // don't care about reply values
    .run()
    .onComplete(d =>{
      system.terminate
          })
*/
  /**
    * Interactive Actor
    */
  object StatusStreamMessages {
    case object StreamInitialized
    case object Acknowledge
    case object StreamCompleted
    case class StreamFailed(reason : Throwable)
  }
  class InteractiveActor extends Actor with ActorLogging {
    import StatusStreamMessages._
    override def receive: Receive = {
      case StreamInitialized =>
        log.info(s"$self : Stream is initialized")
        sender() ! Acknowledge

      case msg: String =>
        log.info(s"$self : received a message $msg")
        sender() ! Acknowledge

      case StreamCompleted =>
        log.info(s"$self : Stream is completed")
        sender() ! StreamCompleted

      case StreamFailed(r) =>
        log.error(s"$self : Stream is failed because : $r")
        sender() ! Failure(new Throwable(r))
    }
  }
  import StatusStreamMessages._
  val actorRef = system.actorOf(Props[InteractiveActor], "interactive-actor")
  val sink = Sink.actorRefWithAck(
    ref = actorRef,
    onInitMessage = StreamInitialized,
    ackMessage = Acknowledge,
    onCompleteMessage = StreamCompleted,
    onFailureMessage = StreamFailed
  )

/*
  Source('A' to 'F')
    .map(_.toString)
    .concat(Source.single(StreamFailed(new Throwable("ERROR !"))))
    .runWith(sink)
*/

  /**
    * Using Source.Queue rather than Sink.actorRef
  */
  val bufferSize = 2
  val elementsToProcess = 2
  val perSeconds = 1.seconds
  val queue: SourceQueueWithComplete[Int] = Source.queue[Int](bufferSize, OverflowStrategy.dropNew)
    .throttle(elementsToProcess, perSeconds)
    .toMat(Sink.foreach(x => println(s"Element $x => ${(x*2)} completed")))(Keep.left)
    .run()
  val source = Source(1 to 10)
    .mapAsync(1) (x => queue.offer(x).map{
      case QueueOfferResult.Enqueued => println(s"$x enqueued.")
      case QueueOfferResult.Dropped => println(s"$x dropped.")
      case QueueOfferResult.Failure(ex) => println(s"Offer failed with : $ex")
      case QueueOfferResult.QueueClosed => println(s"Source Queue closed.")
    })
    .runWith(Sink.ignore)
//    .onComplete(_ => system.terminate)
}
