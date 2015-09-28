package com.nitro.clients

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.nitro.clients.kafka.{KafkaUtils, ImplicitContext}
import com.nitro.messages.NitroMeta
import org.scalacheck.Gen
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.language.postfixOps

class WithKafkaTest extends FunSuite {

  test("test simple producer and consumer") {

    val nMessages = 30
    val nitroMetaTopic = "topic_nitro_meta"

    implicit val ic = new ImplicitContext(ActorSystem("WithKafkaTest"))
    try {

      val nitroMetaMessages =
        Gen.containerOfN[Vector, NitroMeta](
          nMessages,
          NitroMeta._arbitrary
        )
          .sample
          .get



      val received =
        KafkaUtils.withKafka { kafka =>

          Source(nitroMetaMessages)
            .runWith(kafka.produceGeneric[NitroMeta](nitroMetaTopic))

          import scala.concurrent.duration._
          Await.result(
            kafka.consumeGeneric[NitroMeta](nitroMetaTopic)
              .take(nitroMetaMessages.size)
              .runWith(Sink.fold(Seq.empty[NitroMeta])(_ ++ Seq(_))),
            5 seconds
          )
        }

      assert(received.size == nitroMetaMessages.size)

      nitroMetaMessages.zip(received)
        .foreach {
          case (expected, actual) => assert(expected == actual)
        }

    } finally {
      ic.as.shutdown()
      ic.mat.shutdown()
    }

  }

}
