package io.tenmax.cqlkit;

import java.io.File;
import java.util.Optional;

import javax.net.ssl.SSLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.NettySSLOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/**
 * The class to manage the Cassandra Connection.
 */
public class SessionFactory implements AutoCloseable{
    private static SessionFactory instance;

    private Cluster cluster;
    private Session session;

    private SessionFactory(CommandLine commandLine,
                           HierarchicalINIConfiguration cqlshrc) throws SSLException {


        Cluster.Builder builder = Cluster.builder();

        Optional<HierarchicalINIConfiguration> rcOpt = Optional.ofNullable(cqlshrc);

        if(commandLine.hasOption("c")) {
            builder.addContactPoints(commandLine.getOptionValue("c").split(","));
        } else {
            rcOpt.map(rc -> rc.getSection("connection"))
                 .map(conn -> conn.getString("hostname"))
                 .ifPresent(hostName -> {
                     builder.addContactPoints(hostName.split(","));
                 });
        }

        Optional<SubnodeConfiguration> authOpt = rcOpt.map(rc -> rc.getSection("authentication"));
        if(commandLine.hasOption("u")) {
            builder.withCredentials(commandLine.getOptionValue("u"),
                    commandLine.getOptionValue("p"));
        } else {
            String username = authOpt
                    .map(auth -> auth.getString("username"))
                    .orElse(null);
            String password = authOpt
                    .map(auth -> auth.getString("password"))
                    .orElse(null);
            if (username != null && password != null) {
                builder.withCredentials(username, password);
            }
        }

        Optional<SubnodeConfiguration> sslOpt = rcOpt.map(rc  -> rc.getSection("ssl"));
        if (sslOpt.isPresent()) {
            SslContextBuilder sslBuilder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);
            Optional<String> certFile = sslOpt.map(f -> f.getString("certfile"));
            Optional<String> validate = sslOpt.map(f -> f.getString("validate")); // TODO: support validate = false
            if (certFile.isPresent()) 
                sslBuilder.trustManager(new File(certFile.get()));
            SSLOptions sslOptions = new NettySSLOptions(sslBuilder.build());
            builder.withSSL(sslOptions);
        }
        
        if(commandLine.hasOption("fetchSize")) {
            int fetchSize = Integer.parseInt(commandLine.getOptionValue("fetchSize"));
            System.err.println("fetch size=" + fetchSize);
            builder.withQueryOptions(new QueryOptions().setFetchSize(fetchSize));
        }

        // query retry & reconnection policy
        builder.withRetryPolicy(new CustomRetryPolicy(3, 3, 3));
        builder.withReconnectionPolicy(new ExponentialReconnectionPolicy(5000L, 3600*1000L));
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, Integer.getInteger("localMaxCon", 16));
        builder.withPoolingOptions(poolingOptions);
        builder.withReconnectionPolicy(new ConstantReconnectionPolicy(1000));
        
        cluster = builder.build();
        session = cluster.connect();

        // Change the db
        String keyspaceName = null;

        if (commandLine.hasOption("k")) {
            keyspaceName = commandLine.getOptionValue("k");
        } else {
            keyspaceName= authOpt
                    .map(auth -> auth.getString("keyspace"))
                    .orElse(null);
        }
        if(keyspaceName != null) {
            KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspaceName);
            if(keyspaceMetadata == null) {
                System.err.printf("Keyspace '%s' does not exist\n", keyspaceName);
                System.exit(1);
            }
            session.execute("use " + keyspaceName);
        }
    }

    public static class CustomRetryPolicy implements RetryPolicy {

        private final int readAttempts;
        private final int writeAttempts;
        private final int unavailableAttempts;

        public CustomRetryPolicy(int readAttempts, int writeAttempts, int unavailableAttempts) {
            this.readAttempts = readAttempts;
            this.writeAttempts = writeAttempts;
            this.unavailableAttempts = unavailableAttempts;
        }
        
        public void init(Cluster cluster) {
        }

        public void close() {
        }

        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            if (dataRetrieved) {
                return RetryDecision.ignore();
            } else if (nbRetry < readAttempts) {
                return RetryDecision.retry(cl);
            } else {
                return RetryDecision.rethrow();
            }
        }

        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            if (nbRetry < writeAttempts) {
                return RetryDecision.retry(cl);
            }
            return RetryDecision.rethrow();
        }


        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            if (nbRetry < unavailableAttempts) {
                return RetryDecision.tryNextHost(cl);
            }
            return RetryDecision.rethrow();
        }

        public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
            if (e instanceof ConnectionException) {
                return RetryDecision.retry(cl);
            }
            return RetryDecision.rethrow();
        }
    }
    
    public static SessionFactory newInstance(
            CommandLine commandLine,
            HierarchicalINIConfiguration cqlshrc) throws SSLException
    {
        if(instance == null) {
            instance = new SessionFactory(commandLine, cqlshrc);
        }
        return instance;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        session.close();
        cluster.close();
    }
}
