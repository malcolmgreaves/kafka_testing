package com.nitro.clients.kafka

import java.io.ByteArrayOutputStream
import java.util.UUID

import akka.event.LoggingAdapter
import akka.stream.scaladsl.{Source, Sink}
import com.nitro.scalaAvro.runtime.{GeneratedMessageCompanion, Message, GeneratedMessage}
import com.softwaremill.react.kafka.ReactiveKafka
import kafka.serializer.{Decoder, Encoder}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.util.Try
import scala.util.control.NonFatal


trait KafkaBase {
  def produceGeneric[T <: GeneratedMessage with Message[T]](
                                                             topic:   String,
                                                             groupId: String = UUID.randomUUID().toString
                                                             )(implicit companion: GeneratedMessageCompanion[T]): Sink[T, Unit]

  def consumeGeneric[T <: GeneratedMessage with Message[T]](
                                                             topic:   String,
                                                             groupId: String = UUID.randomUUID().toString
                                                             )(implicit companion: GeneratedMessageCompanion[T]): Source[T, Unit]
}

class Kafka(kafkaConfiguration: KafkaConfiguration, logger: LoggingAdapter)(implicit implicitContext: ImplicitContext) extends KafkaBase() {
  import implicitContext._

  private lazy val kafka = new ReactiveKafka(host = kafkaConfiguration.kafkaHost, zooKeeperHost = kafkaConfiguration.zookeeperHost)

  override def produceGeneric[T <: GeneratedMessage with Message[T]](topic: String, groupId: String = UUID.randomUUID.toString)(implicit companion: GeneratedMessageCompanion[T]) =
    Sink(kafka.publish(topic, UUID.randomUUID().toString, new Encoder[T] {
      override def toBytes(t: T): Array[Byte] = {
        logger.info(s"sending $t to kafka topic $topic")
        AvroMessageEncoder.encode(logger)(t)(companion)
      }
    }))

  override def consumeGeneric[T <: GeneratedMessage with Message[T]](topic: String, groupId: String = UUID.randomUUID.toString)(implicit companion: GeneratedMessageCompanion[T]) =
    Source(kafka.consume(topic, UUID.randomUUID().toString, new Decoder[T] {
      override def fromBytes(bytes: Array[Byte]): T = {
        val res = AvroMessageEncoder.decode(logger)(companion)(bytes)
        logger.info(s"read $res from kafka topic $topic")
        res
      }
    }))
}

object AvroMessageEncoder {

  import language.postfixOps
  def encode[T <: GeneratedMessage with Message[T]](logger: LoggingAdapter)(message: T)(implicit companion: GeneratedMessageCompanion[T]): Array[Byte] = Try {
    val baos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(baos, null)
    val writer = new GenericDatumWriter[GenericRecord](companion.schema)
    val gen = message.toMutable
    writer.write(gen, encoder)
    encoder.flush()
    baos.toByteArray
  } recover {
    case NonFatal(t: Throwable) =>
      logger.error(s"Error encoding $message: ${t.getMessage}")
      throw t
  } get

  def decode[T <: GeneratedMessage with Message[T]](logger: LoggingAdapter)(companion: GeneratedMessageCompanion[T])(bytes: Array[Byte]): T = Try {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new GenericDatumReader[GenericRecord](companion.schema)
    val r = reader.read(null, decoder)
    companion.fromMutable(r)
  } recover {
    case NonFatal(t: Throwable) =>
      logger.error(s"Error decoding message into $companion: ${t.getMessage}")
      throw t
  } get
}