package com.nitro.clients.kafka

object KafkaUtils {
  def withKafka[T](f: KafkaBase => T): T = {

    import java.util.ArrayList
    import java.util.Properties

    val embeddedZookeeper = new EmbeddedZookeeper(-1)
    val kafkaPorts = new ArrayList[Integer]()
    // -1 for any available port
    kafkaPorts.add(-1)
    val embeddedKafkaCluster = new EmbeddedKafkaCluster(embeddedZookeeper.getConnection(), new Properties(), kafkaPorts)
    embeddedZookeeper.startup()
    val zkHost = embeddedZookeeper.getConnection()
    embeddedKafkaCluster.startup()

    val kafkaHost = embeddedKafkaCluster.getBrokerList()

    val kafkaConfig = KafkaConfiguration(kafkaHost = kafkaHost, zookeeperHost = zkHost)
    try {
      val k = new Kafka(kafkaConfig, log)
      f(k)
    } finally {
      embeddedKafkaCluster.shutdown()
      embeddedZookeeper.shutdown()
    }
  }
}