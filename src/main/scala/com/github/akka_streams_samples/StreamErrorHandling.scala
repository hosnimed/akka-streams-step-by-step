package com.github.akka_streams_samples

import akka.NotUsed
import akka.actor.{ActorSystem, Kill}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl._
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._


object StreamErrorHandling extends App {

  // Mock akka-http interfaces
  object Http {
    def apply() = this
    def singleRequest(req: HttpRequest) = Future.successful(s"Request to ${req.uri} sent successfully!")
  }
  case class HttpRequest(uri: String)
  case class Unmarshal(b: Any) {
    def to[T]: Future[T] = Promise[T]().future
  }
  case class ServerSentEvent()


  implicit val system = ActorSystem("stream-error-handling")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global
  implicit val timeOut = Timeout(5.seconds)
  /**
    * Delayed restarts with a backoff operator
    */
 val restartedSource = RestartSource.withBackoff(
   minBackoff = 3.seconds,
   maxBackoff = 3.seconds,
   randomFactor = 0.2d,
   maxRestarts = 10
 ){() =>
   Source.fromFutureSource[ServerSentEvent, String]{
     Http()
       .singleRequest(HttpRequest("http://example.com"))
       .flatMap[Source[ServerSentEvent, String]](future=>{
         println(s" $future received.")
         Unmarshal(future).to[Source[ServerSentEvent, String]]
              }
              )
   }
 }

  restartedSource
    .viaMat(KillSwitches.single)(Keep.right)
    .runWith(Sink.ignore)
    .onComplete(_ => system.terminate)

 /* val (event, killSwitch )= restartedSource
    .viaMat(KillSwitches.single[ServerSentEvent])(Keep.both)
    .toMat(Sink.foreach(evt => println(s"Receive event : $evt")))(Keep.left)
    .run()

  Thread.sleep(5000)
  killSwitch.shutdown()*/

}
