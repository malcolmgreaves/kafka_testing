package com.nitro.clients.kafka;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The embedded Zookeeper instance. When instantiated, this class provides
 * complete functionality of a running Zookeeper server. It executed within
 * the JVM: not as an external process.
 *
 * This class should be used in tandem with the EmbeddedKafkaCluster class.
 */
public class EmbeddedZookeeper {
    private final int port;
    private final int tickTime;

    private ServerCnxnFactory factory;
    private File snapshotDir;
    private File logDir;

    public EmbeddedZookeeper() {
        this(-1);
    }

    public EmbeddedZookeeper(final int port) {
        this(port, 500);
    }

    public EmbeddedZookeeper(final int port, final int tickTime) {
        this.port = KafkaUtils.unsafeResolvePort(port);
        if (tickTime <= 0)
            this.tickTime = 0;
        else
            this.tickTime = tickTime;
    }

    /**
     * Creates a new server connection factory with a newly constructed
     * ZooKeeperServer, which itself has access to newly created snapshot
     * and logging (temp) directories.
     */
    public void startup() throws IOException {
        this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
        this.snapshotDir = TestUtils.constructTempDir("embeeded-zk/snapshot");
        this.logDir = TestUtils.constructTempDir("embeeded-zk/log");

        try {
            factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }


    /**
     * Calls shutdown on the internal server connection factory, which, in turn,
     * shuts down the ZooKeeperServer instance created in startup. Also, deletes
     * the snapshot and logging directories created from startup.
     */
    public void shutdown() {
        factory.shutdown();
        try {
            TestUtils.deleteFile(snapshotDir);
        } catch (FileNotFoundException e) {
            // ignore
        }
        try {
            TestUtils.deleteFile(logDir);
        } catch (FileNotFoundException e) {
            // ignore
        }
    }

    public String getConnection() {
        return "localhost:" + port;
    }

    public int getPort() {
        return port;
    }

    public int getTickTime() {
        return tickTime;
    }

    public File getLogDir() {
        return logDir;
    }

    public File getSnapshotDir(){
        return snapshotDir;
    }

    @Override
    public String toString() {
        return "EmbeddedZookeeper{" + "connection=" + getConnection() + '}';
    }
}
