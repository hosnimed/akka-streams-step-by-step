package com.github.akka_streams_samples

import akka.actor.ActorSystem
import akka.pattern.FutureRef
import akka.{Done, NotUsed}
import akka.stream._
import akka.stream.scaladsl._
import com.github.akka_streams_samples.TweetAPI._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

object GraphDSLSample extends App {

  implicit val system: ActorSystem = ActorSystem("akka-tweets")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  final class HiddenDefinitions {
    //#graph-dsl-broadcast
    val writeAuthors: Sink[Author, Future[Done]] = Sink.foreach(author => print(s"Author ${author.name} \n"))
    val writeHashtags: Sink[List[Hashtag], Future[Done]] =Sink.foreach(tags => {
      val allTags =  tags.map(tag => tag.name).reduce(_.concat("\t").concat(_))
      print(s"All Tags: ${allTags}\n")
      })
      //#graph-dsl-broadcast
  }


  val tweets: Source[Tweet, NotUsed] = Source(
    Tweet(Author("melhosni"), System.currentTimeMillis(), "awesome #akka")::
      Tweet(Author("rolandkuhn"), System.currentTimeMillis, "#akka rocks!") ::
      Tweet(Author("patriknw"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("bantonsson"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("drewhk"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("ktosopl"), System.currentTimeMillis, "#akka on the rocks!") ::
      Tweet(Author("mmartynas"), System.currentTimeMillis, "wow #akka !") ::
      Tweet(Author("akkateam"), System.currentTimeMillis, "#akka rocks!") ::
      Tweet(Author("bananaman"), System.currentTimeMillis, "#bananas rock!") ::
      Tweet(Author("appleman"), System.currentTimeMillis, "#apples rock!") ::
      Tweet(Author("drama"), System.currentTimeMillis, "we compared #apples to #oranges!") ::
      Nil
  )
  val hiddenDefinitions = new HiddenDefinitions

  val graph1: RunnableGraph[NotUsed] = RunnableGraph.fromGraph( GraphDSL.create() { implicit graphBuilder =>
    import GraphDSL.Implicits._
    //#junction (fan-out/fan-in)
    val builderCast = graphBuilder.add(Broadcast[TweetAPI.Tweet](2))
    tweets ~> builderCast.in

    builderCast.out(0) ~> Flow[TweetAPI.Tweet].map(_.author) ~> hiddenDefinitions.writeAuthors
    builderCast.out(1) ~> Flow[TweetAPI.Tweet].map(_.hashtags.toList) ~> hiddenDefinitions.writeHashtags


    ClosedShape
  })

  val graph2 = RunnableGraph.fromGraph(GraphDSL.create(){ implicit builder =>
    import GraphDSL.Implicits._

    val in = Source[Int](1 to 5)
    val out: Sink[Int, Future[Int]] = Sink.reduce[Int]((a, b) => a+b)
    val out2 = Sink.foreach(println)

    val bcast = builder.add( Broadcast[Int](outputPorts = 2) )
    val merge = builder.add( Merge[Int](inputPorts = 2) )

    val f1, f2, f3, f4 = Flow[Int].map(_ + 10)

    in ~> f1 ~> bcast ~> f2 ~> merge ~> f3 ~> out2
    bcast ~> f4 ~> merge

    ClosedShape
  })
  /**
    * 1 : 1~>11~><21,21>~><31,31>
    * 2 : 2~>12~><22,22>~><32,32>
    * 3 : 3~>13~><23,23>~><33,33>
    *   ...........
    */
/*  graph2.run()(materializer)*/

  val headSink = Sink.head[Int]
  val middleSink = Sink.reduce[Int]((a,b)=>a+b) //foreachAsync[Int](2)(a => Future{a + 1})
  val bottomSink = Sink.last[Int]
  val sharedFlow = Flow[Int].map(_ * 2)

  val graph3 = RunnableGraph.fromGraph(GraphDSL.create(headSink,middleSink, bottomSink)((_, _, _)) { implicit builder =>
    (hSink, mSink, bSink) =>
    import GraphDSL.Implicits._
    val broadcast = builder.add(Broadcast[Int](outputPorts = 3))
    Source[Int](1 to 5) ~> broadcast.in
      broadcast ~> sharedFlow ~> hSink.in
      broadcast ~> sharedFlow ~> mSink.in
      broadcast ~> sharedFlow ~> bSink.in

    ClosedShape
  })

 /* val (hFuture, mFuture, bFuture) = graph3.run()(materializer)
    hFuture.onComplete(t => {
      println(s"hFuture : $t")
      system.terminate()
    })
    mFuture.onComplete(t => {
      println(s"mFuture : $t")
      system.terminate()
    })
    bFuture.onComplete(t => {
      println(s"bFuture : $t")
      system.terminate()
    })*/

  //  #partial-graph

  //# simple fan-in
  /*  val pickMax = GraphDSL.create(){implicit b =>
      import GraphDSL.Implicits._

      val zip1 = b.add(ZipWith[Int,Int,Int](Integer.sum)) // 2 in, 1 out
      val zip2 = b.add(ZipWith[Int,Int,Int](Integer.sum)) // 2 in, 1 out
      zip2

    }*/

  //  partial-graph#

  /**
    * Source -->    taskIn -> MergePref.in(0)                                   Balance.out(0) -> in.worker.out -> in(0).MergeResult
    *                                            } MergePref.out ~> in.Balance {                                                    } MergeResult.out ~> Sink.foreach(println)
    * Source -->priorTaskIn-> MergePref.preferred                               Balance.out(n) -> in.worker.out -> in(n).MergeResult
    *
    */

  //#graph-dsl-components-shape
  case class PriorityWorkerTaskShape[In, Out] (
                                            tasksIn: Inlet[In],
                                            priorTasks: Inlet[In],
                                            resultOut: Outlet[Out]) extends Shape {

    override def inlets: immutable.Seq[Inlet[_]] = tasksIn :: priorTasks :: Nil

    override def outlets: immutable.Seq[Outlet[_]] = resultOut :: Nil

    override def deepCopy(): Shape = PriorityWorkerTaskShape (
      tasksIn.carbonCopy(),
      priorTasks.carbonCopy(),
      resultOut.carbonCopy()
    )
  }
  // Another short way to do create the Shape using predefined FanInShape
  /*class PriorityWorkerFanInShape[In, Out]
    (_init: FanInShape.Init[Out] = FanInShape.Name("PriorityWorkerFanInWorkerPool"))
    extends FanInShape[Out](_init){

    override protected def construct(init: FanInShape.Init[Out]): FanInShape[Out] = new PriorityWorkerFanInShape(init)

    val tasksIn = new Inlet[In]("Tasks")
    val priorTasks = new Inlet[In]("PriorTasks")
    //out will be auto created FanInShape[...,Out]
  }*/
  // Wire up the Graph
  object PriorityWorkerTaskPool {
    def apply[In, Out](
                        // simple worker flow
                        worker: Flow[In, Out, Any],
                        numberOfWorker: Int): Graph[PriorityWorkerTaskShape[In, Out], NotUsed] = {

      GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        //First, we will merge incoming normal and priority jobs using MergePreferred
        val priorMerge = b.add(MergePreferred[In](1))
        //then we will send the jobs to a Balance junction which will fan-out to a configurable number of workers (flows)
        val balance = b.add(Balance[In](numberOfWorker))
        //finally we merge all these results together and send them out through our only output port
        val mergedResult = b.add(Merge[Out](numberOfWorker))

        //after merging prior and normal tasks, send them to balance (fan-out)
        priorMerge ~> balance
        //connect every Balance Output via a Worker to a MergeResult's Input
        for (i <- 0 until numberOfWorker)
          balance.out(i) ~> worker ~> mergedResult.in(i)

        PriorityWorkerTaskShape(
          tasksIn = priorMerge.in(0),
          priorTasks = priorMerge.preferred,
          resultOut = mergedResult.out
        )
      }
    }
  }

  /**
    *Source -->    priorityPool1.taskIn -> MergePref.in(0)                            Balance.out(0) -> in.worker.out -> in(0).MergeResult
    *                                                 } MergePref.out ~> in.Balance {                                                     } MergeResult.out ~> tasksIn.priorityPool2.resultOut ~> Sink.foreach(println)
    *Source -->priorityPool1.priorTaskIn-> MergePref.preferred                        Balance.out(n) -> in.worker.out -> in(n).MergeResult
    *
    */


 // construct a runnable the graph
  val worker1 = Flow[String].map("Processing Setp 1 "+_)
  val worker2 = Flow[String].map("Processing Setp 2 "+_)

  val g =  RunnableGraph.fromGraph(GraphDSL.create() { implicit b ⇒
    import GraphDSL.Implicits._

    val priorityPool1 = b.add(PriorityWorkerTaskPool(worker1, 2))
    val priorityPool2 = b.add(PriorityWorkerTaskPool(worker2, 1))

    Source(1 to 20).map("job: " + _) ~> priorityPool1.tasksIn
    Source(1 to 20).map("priority job: " + _) ~> priorityPool1.priorTasks

    priorityPool1.resultOut ~> priorityPool2.tasksIn
    Source(1 to 20).map("one-step, priority " + _) ~> priorityPool2.priorTasks

    priorityPool2.resultOut ~> Sink.foreach(println)
    ClosedShape
  })
//  g.run()
  //graph-dsl-components-shape#

  //
  object GraphMaterializedValues {

    import GraphDSL.Implicits._

    val foldFlow: Flow[Int, Int, Future[Int]] = Flow.fromGraph(
      GraphDSL.create(Sink.fold[Int, Int](0)(Integer.sum)) { implicit builder ⇒ fold ⇒
      FlowShape(fold.in, builder.materializedValue.mapAsyncUnordered[Int](4)(identity).outlet)
    })

  }
  val flow: Flow[Int, Int, Future[Int]] = GraphMaterializedValues.foldFlow;
  val source: Source[Int, Future[Int]] = Source[Int](1 to 10 ).viaMat(flow)(Keep.right)
  val graph: RunnableGraph[Future[Int]] = source.to(Sink.ignore)
/*
  val done: Future[Int] = graph.run()
  done.onComplete(future => {
    if (future.isSuccess) println(future.get)
    system.terminate
  })
  */
  //#zipping-live
val liveGraph = RunnableGraph.fromGraph(GraphDSL.create(){ implicit builder =>
  import GraphDSL.Implicits._

  val zip = builder.add(ZipWith[Int, Int, Int]((backward, feedback) => backward))
  val broadcast = builder.add(Broadcast[Int](2))
  val concat = builder.add(Concat[Int](2))

  Source(1 to 10) ~> zip.in0 /*backward arc*/
  zip.out.map(i => {println(i); i}) ~> broadcast ~> Sink.ignore
  /*feedback arc*/   zip.in1 <~ concat <~ Source.single[Int](0) /* just to kick-off the cycle*/
                                concat <~ broadcast

  ClosedShape
})
  liveGraph.run()
  //zipping-live#
}

