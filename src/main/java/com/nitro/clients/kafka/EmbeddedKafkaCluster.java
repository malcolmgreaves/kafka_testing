package com.nitro.clients.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * The embedded Kafka instance. When instantiated, this class provides complete
 * functionality of a running Kafka cluster. It executes within the JVM: no
 * external processes are created.
 *
 * When creating this instance, it is critical to also create a EmbeddedZookeeper
 * instance. Zookeeper needs to know about Kafka and the two must communicate
 * with one another in order to bring up a full, working Kafka cluster.
 */
public class EmbeddedKafkaCluster {
    private final List<Integer> ports;
    private final String zkConnection;
    private final Properties baseProperties;

    private final String brokerList;

    private final List<KafkaServer> brokers;
    private final List<File> logDirs;

    public EmbeddedKafkaCluster(final String zkConnection) {
        this(zkConnection, new Properties());
    }

    public EmbeddedKafkaCluster(
            final String zkConnection,
            final Properties baseProperties
    ) {
        this(zkConnection, baseProperties, Collections.singletonList(-1));
    }

    public EmbeddedKafkaCluster(
            final String zkConnection,
            final Properties baseProperties,
            final List<Integer> ports
    ) {
        this.zkConnection = zkConnection;
        this.ports = resolvePorts(ports);
        this.baseProperties = baseProperties;

        this.brokers = new ArrayList<>();
        this.logDirs = new ArrayList<>();

        this.brokerList = constructBrokerList(this.ports);
    }

    public static List<Integer> resolvePorts(final List<Integer> ports) {
        final List<Integer> resolvedPorts = new ArrayList<>();
        for (final Integer port : ports) {
            resolvedPorts.add(KafkaUtils.unsafeResolvePort(port));
        }
        return resolvedPorts;
    }

    private String constructBrokerList(final List<Integer> ports) {
        final StringBuilder sb = new StringBuilder();
        for (final Integer port : ports) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append("localhost:").append(port);
        }
        return sb.toString();
    }

    /**
     * Creates a (temporary) logging directory, sets all Zookeeper and broker
     * connection information, and starts all brokers.
     */
    public void startup() {
        for (int i = 0; i < ports.size(); i++) {
            final Integer port = ports.get(i);
            final File logDir = TestUtils.constructTempDir("kafka-local");

            final Properties properties = new Properties();
            properties.putAll(baseProperties);
            properties.setProperty("zookeeper.connect", zkConnection);
            properties.setProperty("broker.id", String.valueOf(i + 1));
            properties.setProperty("host.name", "localhost");
            properties.setProperty("port", Integer.toString(port));
            properties.setProperty("log.dir", logDir.getAbsolutePath());
            properties.setProperty("log.flush.interval.messages", String.valueOf(1));

            final KafkaServer broker = startBroker(properties);

            brokers.add(broker);
            logDirs.add(logDir);
        }
    }


    /**
     * Starts a single broker using the supplied Properties.
     */
    private KafkaServer startBroker(final Properties props) {
        final KafkaServer server = new KafkaServer(
                new KafkaConfig(props),
                new SystemTime()
        );
        server.startup();
        return server;
    }

    /**
     * Returns a new Properties instance containing the
     * "metadata.broker.list" and "zookeeper.connect" information.
     */
    public Properties getProps() {
        final Properties props = new Properties();
        props.putAll(baseProperties);
        props.put("metadata.broker.list", brokerList);
        props.put("zookeeper.connect", zkConnection);
        return props;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public String getZkConnection() {
        return zkConnection;
    }

    /**
     * Asks each broker to shutdown and deletes all log files.
     */
    public void shutdown() {
        for (final KafkaServer broker : brokers) {
            try {
                broker.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        for (final File logDir : logDirs) {
            try {
                TestUtils.deleteFile(logDir);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return "EmbeddedKafkaCluster{" + "brokerList='" + brokerList + '\'' + '}';
    }
}