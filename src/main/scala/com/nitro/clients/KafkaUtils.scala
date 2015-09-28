package com.nitro.clients.kafka

import java.util
import util.Properties

import akka.event.LoggingAdapter
import com.typesafe.scalalogging.Logger
import org.slf4j.helpers.NOPLogger

import scala.language.implicitConversions

object KafkaUtils {

  case class KProps(p: Properties, log: Logger)

  object KProps {
    val empty: KProps =
      KProps(
        p = new Properties(),
        log = Logger(NOPLogger.NOP_LOGGER)
      )
  }

  def withKafka[T](f: KafkaBase => T, kp: KProps = KProps.empty): T = {

    // create embedded Kafka and Zookeeper instances
    val embeddedZookeeper = new EmbeddedZookeeper(-1)
    val embeddedKafkaCluster = new EmbeddedKafkaCluster(
      embeddedZookeeper.getConnection,
      new Properties(),
      {
        val kafkaPorts = new util.ArrayList[Integer]()
        // -1 for any available port
        kafkaPorts.add(-1)
        kafkaPorts
      }
    )
    embeddedZookeeper.startup()
    val zkHost = embeddedZookeeper.getConnection
    embeddedKafkaCluster.startup()

    // run the KafkaBase to T function
    val kafkaHost = embeddedKafkaCluster.getBrokerList

    val kafkaConfig = KafkaConfiguration(kafkaHost = kafkaHost, zookeeperHost = zkHost)
    try {
      import TypesafeLoggerIsLoggingAdapter.Implicits._
      val k = new Kafka(kafkaConfig, kp.log)
      f(k)

    } finally {
      // shutdown and destroy our system
      embeddedKafkaCluster.shutdown()
      embeddedZookeeper.shutdown()
    }
  }

  object TypesafeLoggerIsLoggingAdapter {
    object Implicits {
      implicit def logToLogAdapt(l: Logger): LoggingAdapter =
        new LoggingAdapter {

          // logging methods

          override protected def notifyDebug(message: String): Unit =
            l.debug(message)

          override protected def notifyInfo(message: String): Unit =
            l.info(message)


          override protected def notifyWarning(message: String): Unit =
            l.warn(message)

          override protected def notifyError(message: String): Unit =
            l.error(message)

          override protected def notifyError(cause: Throwable, message: String): Unit =
            l.error(message, cause)

          // state checking methods

          override def isDebugEnabled: Boolean =
            l.underlying.isDebugEnabled

          override def isInfoEnabled: Boolean =
            l.underlying.isInfoEnabled

          override def isWarningEnabled: Boolean =
            l.underlying.isWarnEnabled

          override def isErrorEnabled: Boolean =
            l.underlying.isErrorEnabled
        }
    }
  }
}