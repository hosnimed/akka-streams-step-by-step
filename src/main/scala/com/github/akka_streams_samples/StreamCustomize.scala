package com.github.akka_streams_samples

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.japi.function
import akka.stream._
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.stage._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration

object StreamCustomize extends App {


  implicit val system = ActorSystem("stream-customize")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global

  /**
    * NumberSource
    */
  class NumberSource extends GraphStage[SourceShape[Int]] {
    val out:Outlet[Int] = Outlet("NumberSource")

    override def shape: SourceShape[Int] = SourceShape[Int](out = out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape = shape) with StageLogging {
      var elem = 1
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          push(out, elem)
          log.info(s" Push ${elem}")
          elem += 1
        }
        override def onDownstreamFinish()= {
          log.info("NumberSource : onDownstreamFinish")
          complete(out)
        }
      })
    }
  }
  // construct a graph
  val sourceGraph : Graph[SourceShape[Int], NotUsed] = new NumberSource
  // construct the source
  val numberSource: Source[Int, NotUsed] = Source.fromGraph(sourceGraph)

//  numberSource.take(10).runFold(0)(_+_) onComplete println
//  numberSource.take(20).filter(_ > 10).runFold(0)(_+_).onComplete(f => { println(f) ; system.terminate })

  /**
    * PrinterSink
    */
  class PrinterSink extends GraphStage[SinkShape[String]] {
    val in : Inlet[String] = Inlet("PrinterSink")

    override def shape: SinkShape[String] = SinkShape[String](in = in)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with StageLogging {

      override def preStart() = pull(in)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          log.info(s"Receive Element : ${grab(in)}")
          pull(in)
        }
        override def onUpstreamFinish(): Unit = {
          log.info("PrinterSink : onUpstreamFinish")
          cancel(in)
        }
      })
    }
  }
  // construct a graph
  val sinkGraph : GraphStage[SinkShape[String]] = new PrinterSink
  // construct the sink
  val printerSink : Sink[String, NotUsed] = Sink.fromGraph(sinkGraph)
 /* numberSource
    .take(10)
    .map(_.toString)
    .runWith(printerSink)*/

  /**
    * MapFlow
  */
  class MapFlow[A, B](f : A => B) extends GraphStage[FlowShape[A,B]] {
    val in = Inlet[A]("MapFlow.in")
    val out = Outlet[B]("MapFlow.out")

    override val shape = FlowShape.of[A, B](in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          push(out, f(grab(in)))
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }

  }
  // construct a graph
  val mapFlowGraph = new MapFlow[Int, String](i => i.toString()+"-" )
  // construct the flow
  val mapFlow: Flow[Int, String, NotUsed] = Flow.fromGraph(mapFlowGraph)
/*  numberSource
    .take(10)
    .via(mapFlow)
    .runWith(printerSink)*/

  /**
    * Timer Gate
  */
  class TimerGate[T](sleep : FiniteDuration) extends GraphStage[FlowShape[T, T]]  {

    val in = Inlet[T]("TimerGate.in")
    val out = Outlet[T]("TimerGate.out")

    override def shape: FlowShape[T, T] = FlowShape.of[T,T](in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogicWithLogging(shape) {
      var isGateOpen = false
      setHandler(in, new InHandler {
        log.info("In handler :")
        override def onPush(): Unit = {
          val elem = grab(in)
          // if gate is open, continue pulling
          if (isGateOpen){
              log.info("Gate is still open, continue pulling")
              pull(in)
          }
          //else push and sleep
          else {
            log.info("Gate is closed, push the element and sleep")
            push(out, elem)
//            isGateOpen = true
//            scheduleOnce("gateHandler", sleep)
          }
        }
        })
      setHandler(out, new OutHandler {
          log.info("Out handler :")
          override def onPull(): Unit = pull(in)
        })

      override def onTimer(timerKey: Any): Unit  = isGateOpen = false

      }

    // construct a graph
    val timerGateFlowGraph = new TimerGate[Int](FiniteDuration(1, TimeUnit.SECONDS))
    // construct the flow
    val timerGateFlow: Flow[Int, Int, NotUsed] = Flow.fromGraph(timerGateFlowGraph)
    /*numberSource
      .take(10)
      .via(timerGateFlow)
      .via(mapFlow)
      .runWith(printerSink)*/

    /**
      * Custom materialized values
    */
  class FirstValue[A] extends GraphStageWithMaterializedValue[ FlowShape[A,A], Future[A] ] {

      val in = Inlet[A]("FirstValue.in")
      val out = Outlet[A]("FirstValue.out")

      val shape = FlowShape.of(in, out)

      override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[A]) = {
        val promise = Promise[A]()
        val logic = new GraphStageLogicWithLogging(shape) {
          setHandler(in, new InHandler {
            override def onPush(): Unit = {
              val elem = grab(in)
              promise.success(elem)
              push(out, elem)
              log.info(s" First Element : ${elem} sent")

              // replace handler with one that only forwards elements
              setHandler(in, new InHandler {
                override def onPush(): Unit = {
                  push(out, grab(in))
                }
              })

            }
          })
          setHandler(out, new OutHandler {
            override def onPull(): Unit = pull(in)
          })
        }
        (logic, promise.future)
      }
  }
    val firstValGraph = new FirstValue[Int]
    val firstValFlow: Flow[Int, Int, Future[Int]] = Flow.fromGraph(firstValGraph)
    val firstVal: Source[Int, Future[Int]] = Source(1 to 10)
      .viaMat(firstValFlow)(Keep.right)
    firstVal
//      .runWith(Sink.ignore).onComplete(_ => system.terminate())
      .map(_.toString)
      .toMat(printerSink)(Keep.left)
      .run()
      .onComplete(_ => system.terminate())

  }
}
