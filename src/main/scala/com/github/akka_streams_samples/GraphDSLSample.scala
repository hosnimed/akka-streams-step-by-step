package com.github.akka_streams_samples

import akka.actor.ActorSystem
import akka.{Done, NotUsed}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl._
import com.github.akka_streams_samples.TweetAPI._

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

  val (hFuture, mFuture, bFuture) = graph3.run()(materializer)
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
    })

}
