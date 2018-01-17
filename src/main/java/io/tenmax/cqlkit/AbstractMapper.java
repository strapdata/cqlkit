package io.tenmax.cqlkit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.reflect.TypeToken;

/**
 * The base case of Mappers. A mapper is map the Cassandra row to a specific format.
 *
 */
public abstract class AbstractMapper {
    protected CommandLine commandLine;
    protected HierarchicalINIConfiguration cqlshrc;
    protected boolean lineNumberEnabled = false;
    protected boolean isRangeQuery = true;

    protected AtomicInteger lineNumber = new AtomicInteger(1);
    protected Cluster cluster;
    protected Session session;
    protected List<ColumnMetadata> primaryKey;
    private AtomicInteger completeJobs = new AtomicInteger(0);
    private  int totalJobs;
    private AtomicInteger fileCount = new AtomicInteger(0);
    private long maxLinePerFile = 100000L;
    boolean compressed = false;
    String filePrefix;
    
    protected void prepareOptions(Options options) {
        OptionGroup queryGroup = new OptionGroup();

        queryGroup.addOption(Option
                .builder("q")
                .longOpt("query")
                .hasArg(true)
                .argName("CQL")
                .desc("The CQL query to execute. If specified, it overrides FILE and STDIN.")
                .build());
        queryGroup.addOption(Option
                .builder()
                .longOpt("query-ranges")
                .hasArg(true)
                .argName("CQL")
                .desc("The CQL query would be splitted by the token ranges. " +
                        "WHERE clause is not allowed in the CQL query")
                .build());
        queryGroup.addOption(Option
                .builder()
                .longOpt("query-partition-keys")
                .hasArg(true)
                .argName("TABLE")
                .desc("Query the partition key(s) for a column family.")
                .build());
        options.addOptionGroup(queryGroup);


        options.addOption( "c", true, "The contact point. if use multi contact points, use ',' to separate multi points" );
        options.addOption( "u", true, "The user to authenticate." );
        options.addOption( "p", true, "The password to authenticate." );
        options.addOption( "k","keysapce", false, "The keyspace to use." );
        options.addOption( "v", "version", false, "Print the version" );
        options.addOption( "d", "debug", false, "Debug queries" );
        options.addOption("local", false, "Dump only local token ranges" );
        options.addOption( "z", "compress", false, "GZipped compressed output file" );
        options.addOption( "h", "help", false, "Show the help and exit" );

        options.addOption(Option.builder()
                .longOpt("cqlshrc")
                .hasArg(true)
                .desc("Use an alternative cqlshrc file location, path.")
                .build());

        options.addOption(Option
                .builder()
                .longOpt("consistency")
                .hasArg(true)
                .argName("LEVEL")
                .desc("The consistency level. The level should be 'any', 'one', 'two', 'three', 'quorum', 'all', 'local_quorum', 'each_quorum', 'serial' or 'local_serial'.")
                .build());

        options.addOption(Option
                .builder()
                .longOpt("fetchSize")
                .hasArg(true)
                .argName("SIZE")
                .desc("The fetch size. Default is " + QueryOptions.DEFAULT_FETCH_SIZE)
                .build());

        options.addOption(Option.builder()
                .longOpt("date-format")
                .hasArg(true)
                .desc("Use a custom date format. Default is \"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"")
                .build());

        options.addOption(Option.builder()
                .longOpt("prefix")
                .hasArg(true)
                .desc("Output filename prefix (required when maxlines or compressed). Default is stdout")
                .build());
        
        options.addOption(Option.builder()
                .longOpt("maxline")
                .hasArg(true)
                .desc("Output lines per file. Default is 100000.")
                .build());
        
        options.addOption( "P", "parallel", true, "The level of parallelism to run the task. Default is sequential." );
    }

    abstract protected void printHelp(Options options);

    abstract protected void printVersion();

    protected void head(
            ColumnDefinitions columnDefinitions,
            PrintStream out){
    }

    abstract protected String map(Row row);

    abstract protected String fileExtension();
    
    public void start(String[] args) {
        commandLine = parseArguments(args);
        cqlshrc = parseCqlRc();
        run();
    }

    private CommandLine parseArguments(String[] args) {

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        prepareOptions(options);
        CommandLine commandLine = null;

        try {
            // parse the command line arguments
            commandLine = parser.parse( options, args );

            // validate that block-size has been set
            if( commandLine.hasOption( "h" ) ) {
                printHelp(options);
            } else if( commandLine.hasOption( "v" ) ) {
                printVersion();
            } else {

            }

            if( commandLine.hasOption("consistency")) {
                String consistency = commandLine.getOptionValue("consistency");
                try {
                    ConsistencyLevel.valueOf(consistency.toUpperCase());
                } catch (Exception e) {
                    System.err.println("Invalid consistency level: " + consistency);
                    printHelp(options);
                }
            }

            if( commandLine.hasOption("date-format")) {
                String pattern = commandLine.getOptionValue("date-format");
                try {
                    RowUtils.setDateFormat(pattern);
                } catch (Exception e) {
                    System.err.println("Invalid date format: " + pattern);
                    printHelp(options);
                }
            }
            
            if( commandLine.hasOption("prefix")) {
                this.filePrefix = commandLine.getOptionValue("prefix");
                if( commandLine.hasOption("maxline")) {
                    this.maxLinePerFile = Long.parseLong(commandLine.getOptionValue("maxline"));
                }
            } else {
                if( commandLine.hasOption("maxline")) {
                    System.err.println("maxline requires a filename prefix.");
                    printHelp(options);
                }
            }
            
        } catch (ParseException e) {
            System.out.println( "Unexpected exception:" + e.getMessage() );
            System.exit(1);
        }
        return commandLine;
    }

    private HierarchicalINIConfiguration parseCqlRc() {



        File file = new File(System.getProperty("user.home") + "/.cassandra/cqlshrc");
        if (commandLine.hasOption("cqlshrc")) {
            file = new File(commandLine.getOptionValue("cqlshrc"));
            if(!file.exists()) {
                System.err.println("cqlshrc file not found: " + file);
                System.exit(-1);
            }
        }

        if(file.exists()) {
            try {
                HierarchicalINIConfiguration configuration = new HierarchicalINIConfiguration(file);
                return configuration;
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
        }

        return null;
    }

    private PrintStream newFile(ResultSet rs) throws FileNotFoundException, CompressorException {
        PrintStream out = System.out;
        if (commandLine.hasOption("prefix")) {
            if (commandLine.hasOption("z")) {
                out = new PrintStream( 
                    new CompressorStreamFactory()
                        .createCompressorOutputStream(CompressorStreamFactory.GZIP, 
                            new FileOutputStream(filePrefix + "-"+ fileCount.incrementAndGet()+"."+fileExtension()+".gz")),
                        true);
            } else {
                out = new PrintStream(new FileOutputStream(filePrefix + "-"+ fileCount.incrementAndGet()+"."+fileExtension()));
            }
        }
        head(rs.getColumnDefinitions(), out);
        return out;
    }
    
    private void run() {
        BufferedReader in = null;

        boolean parallel = false;
        int parallelism = 1;
        Executor executor = null;
        
        if(commandLine.hasOption("P")) {
            parallelism = Integer.parseInt(commandLine.getOptionValue("parallel"));
        } else if(commandLine.hasOption("query-ranges") ||
                  commandLine.hasOption("query-partition-keys")) {
            parallelism = Runtime.getRuntime().availableProcessors();
        }

        if(parallelism > 1) {
            executor = new ForkJoinPool(parallelism);
            parallel = true;
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        try(SessionFactory sessionFactory = SessionFactory.newInstance(commandLine, cqlshrc)) {
            cluster = sessionFactory.getCluster();
            session = sessionFactory.getSession();

            // The query source
            Iterator<String> cqls = null;
            if (commandLine.hasOption("q")) {
                cqls = query(sessionFactory);
            } else if (commandLine.hasOption("query-partition-keys")) {
                cqls = queryByPartionKeys(sessionFactory);
            } else if (commandLine.hasOption("query-ranges")) {
                cqls = queryByRange(sessionFactory);
            } else {
                if (commandLine.getArgs().length > 0) {
                    // from file input
                    in = new BufferedReader(
                            new FileReader(commandLine.getArgs()[0]));
                } else {
                    // from standard input
                    in = new BufferedReader(
                            new InputStreamReader(System.in));
                }
                cqls = in.lines().iterator();
            }

            
            lineNumberEnabled = commandLine.hasOption("l");

            isRangeQuery = commandLine.hasOption("query-partition-keys") ||
                           commandLine.hasOption("query-ranges");


            ConsistencyLevel consistencyLevel =
                    commandLine.hasOption("consistency") ?
                    ConsistencyLevel.valueOf(commandLine.getOptionValue("consistency").toUpperCase()) :
                    ConsistencyLevel.ONE;

            // Query
            while(cqls.hasNext()) {
                final String cql = cqls.next().trim();

                if(cql.isEmpty()) {
                    continue;
                }

                
                final boolean _parallel = parallel;
                Runnable task = () -> {
                    int retry = 3;
                    int retryCount = 0;
                    int lineCount = 0;
                    try {
                        while(true) {
                            try {
                                SimpleStatement stmt = new SimpleStatement(cql);
                                stmt.setConsistencyLevel(consistencyLevel);
                                ResultSet rs = session.execute(stmt);

                                PrintStream out = newFile(rs);
                                for(Row row : rs) {
                                    out.println(map(row));
                                    lineCount++;
                                    if (lineCount > this.maxLinePerFile) {
                                        out.flush();
                                        out.close();
                                        out = newFile(rs);
                                        lineCount = 0;
                                    }
                                }
                            } catch (Exception e) {
                                System.err.print("error: "+e);
                                if (retryCount < retry) {
                                    retryCount++;
                                    System.err.printf("%s - Retry %d cql: %s\n", new Date(), retryCount, cql);
                                    try {
                                        Thread.sleep(3000);
                                    } catch (InterruptedException e1) {
                                    }
                                    continue;
                                }
                                System.err.println("Error when execute cql: " + cql);
                                e.printStackTrace();
                                System.exit(1);
                            }

                            break;
                        }
                    } finally {
                        if(_parallel) {
                            System.err.printf("Progress: %d/%d\n",
                                    completeJobs.incrementAndGet(),
                                    totalJobs);
                        }
                    }
                };

                if(parallel) {
                    futures.add(CompletableFuture.runAsync(task, executor));
                } else {
                    task.run();
                }
                totalJobs++;
            }

            // Wait for all futures completion
            if(parallel) {
                CompletableFuture
                        .allOf(futures.toArray(new CompletableFuture[]{}))
                        .join();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(in != null) {
                try {
                    in.close();
                } catch (IOException e) {}
            }
        }
    }

    private List<Long> getLocalTokenRanges() {
        List<Long> localTokens = null;
        if (commandLine.hasOption("local")) {
            Row row = session.execute("SELECT tokens FROM system.local").one();
            localTokens = row.getSet(0, TypeToken.of(String.class)).stream()
                .map(t -> Long.parseLong(t)).collect(Collectors.toList());
            if (commandLine.hasOption("d")) 
                System.err.println("local token: " + localTokens);
        }
        return localTokens;
    }
    
    private Iterator<String> queryByRange(SessionFactory sessionFactory) {
        Iterator<String> cqls;

        String query = commandLine.getOptionValue("query-ranges");

        if(query.contains("where") || query.contains("WHERE")) {
            System.err.println("WHERE is not allowed in query");
            System.exit(1);
        }

        String keyspace = sessionFactory.getSession().getLoggedKeyspace();
        String table = null;
        List<String> strings = parseKeyspaceAndTable(query);
        if (strings.size() == 1) {
            table = strings.get(0);
        } else {
            keyspace = strings.get(0);
            table = strings.get(1);
        }

        if(table == null) {
            System.err.println("Invalid query: " + query);
        }

        if(keyspace == null) {
            keyspace = session.getLoggedKeyspace();
            if(keyspace == null) {
                System.err.println("no keyspace specified");
                System.exit(1);
            }
        }

        this.primaryKey = cluster
                .getMetadata()
                .getKeyspace(keyspace)
                .getTable(table)
                .getPrimaryKey();
        
        List<String> partitionKeys = cluster
                .getMetadata()
                .getKeyspace(keyspace)
                .getTable(table)
                .getPartitionKey()
                .stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());

        List<Long> localTokens = getLocalTokenRanges();
        
        if (commandLine.hasOption("debug"))
            System.err.println("token ranges: "+cluster.getMetadata().getTokenRanges());
        // Build the cql
        cqls = cluster.getMetadata()
                .getTokenRanges()
                .stream()
                .filter(tokenRange -> commandLine.hasOption("local") && localTokens.contains(tokenRange.getStart().getValue()))
                .flatMap(tokenRange -> {
                    ArrayList<String> cqlList = new ArrayList<>();
                    for (TokenRange subrange : tokenRange.unwrap()) {
                        String token = QueryBuilder.token(partitionKeys.toArray(new String[]{}));

                        String cql = String.format("%s where %s > %d and %s <= %d",
                                query,
                                token,
                                subrange.getStart().getValue(),
                                token,
                                subrange.getEnd().getValue());

                        cqlList.add(cql);

                    }

                    if (commandLine.hasOption("debug"))
                        System.err.println(String.join("\n", cqlList));
                    return cqlList.stream();
                })
                .iterator();
        return cqls;
    }

    private Iterator<String> query(SessionFactory sessionFactory) {
        Iterator<String> cqls;
        String keyspace = sessionFactory.getSession().getLoggedKeyspace();
        String table = null;
        String query = commandLine.getOptionValue("q");
        
        List<String> strings = parseKeyspaceAndTable(query);
        if (strings.size() == 1) {
            table = strings.get(0);
        } else {
            keyspace = strings.get(0);
            table = strings.get(1);
        }
        
        this.primaryKey = cluster
                .getMetadata()
                .getKeyspace(keyspace)
                .getTable(table)
                .getPrimaryKey();
        
        return Arrays.asList(query).iterator();
    }
    
    private Iterator<String> queryByPartionKeys(SessionFactory sessionFactory) {
        Iterator<String> cqls;
        String keyspace = session.getLoggedKeyspace();
        String table = commandLine.getOptionValue("query-partition-keys");
        if(keyspace == null) {
            System.err.println("no keyspace specified");
            System.exit(1);
        }

        TableMetadata tableMetadata = cluster.getMetadata().getKeyspace(keyspace).getTable(table);
        if(tableMetadata == null) {
            System.err.printf("table '%s' does not exist\n", table);
            System.exit(1);
        }

        this.primaryKey = cluster
                .getMetadata()
                .getKeyspace(keyspace)
                .getTable(table)
                .getPrimaryKey();
        
        List<String> partitionKeys = tableMetadata
                .getPartitionKey()
                .stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());

        List<Long> localTokens = getLocalTokenRanges();
        
        if (commandLine.hasOption("debug"))
            System.err.println("token ranges: "+cluster.getMetadata().getTokenRanges());
        // Build the cql
        cqls = cluster.getMetadata()
            .getTokenRanges()
            .stream()
            .filter(tokenRange -> commandLine.hasOption("local") && localTokens.contains(tokenRange.getStart().getValue()))
            .flatMap(tokenRange -> {
                ArrayList<String> cqlList = new ArrayList<>();
                for (TokenRange subrange : tokenRange.unwrap()) {
                    String token = QueryBuilder.token(partitionKeys.toArray(new String[]{}));

                    Select.Selection selection = QueryBuilder
                        .select()
                        .distinct();
                    partitionKeys.forEach(column -> selection.column(column));

                    String cql = selection
                            .from(commandLine.getOptionValue("query-partition-keys"))
                            .where(QueryBuilder.gt(token, subrange.getStart().getValue()))
                            .and(QueryBuilder.lte(token, subrange.getEnd().getValue()))
                            .toString();

                    cqlList.add(cql);
                }

                if (commandLine.hasOption("debug"))
                    System.err.println(String.join("\n", cqlList));
                return cqlList.stream();
            })
            .iterator();
        return cqls;
    }

    public static List<String> parseKeyspaceAndTable(String query) {
        String regex = "(select|SELECT) .* (from|FROM) ((?<keyspace>[a-zA-Z_0-9]*)\\.)?(?<table>[a-zA-Z_0-9]*)\\W?.*";

        String keyspace = null;
        String table = null;

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(query);
        if(matcher.find()) {
            keyspace = matcher.group("keyspace");
            table = matcher.group("table");

        }

        return Arrays.asList(keyspace, table);
    }
}
