package io.snappydata.cassandra.readcdc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.google.common.collect.Sets;

import com.datastax.driver.core.Cluster;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CDCDaemon
{
    public static final String SCHEMA_NAME = "system_schema";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    private final Path commitlogDirectory = Paths.get("/home/hemant/install/apache-cassandra-3.10/data/commitlog");
    private final Path cdcRawDirectory;
    private final CDCHandler handler = new PushToSnappy(); //new LogHandler1();
    private final Set<UUID> unknownCfids = Sets.newSetFromMap(new ConcurrentHashMap<UUID, Boolean>());
    private final String cass_keyspace;
    private final String cass_table;
    private Connection conn;

    private CDCDaemon(String[] args)
    {
        this.cass_table = args[2];
        this.cass_keyspace = args[1];
        cdcRawDirectory = Paths.get(args[0]);
        String snappyconn = "";
        if (args.length == 4) {
            snappyconn = args[3];
        } else if (args.length == 3) {
            snappyconn = "localhost:1527";
        } else {
            System.out.println("CDCDaemon cdcDirectoryPath KeySpace tableName snappyClientConn");
            return;
        }

        conn = getConnection("jdbc:snappydata://" + snappyconn + "/host-data=false;");

        Config.setClientMode(true);
    }

    public Connection getConnection(String url) {
        Connection con = null;
        String driver = "io.snappydata.jdbc.ClientDriver";
        try {
            Class.forName(driver);
        } catch (java.lang.ClassNotFoundException e) {
            System.err.print("ClassNotFoundException: ");
            System.err.println(e.getMessage());
            System.exit(3);
        }

        try {
            con = DriverManager.getConnection(url);
        } catch (SQLException ex) {
            System.err.println("SQLException: " + ex.getMessage());
            System.exit(3);
        }

        return con;
    }

    public static void main(String[] args)
    {
        new CDCDaemon(args).start();
    }

    private void tryRead(Path p, boolean canDelete, boolean canReload)
    {
        try
        {
            DatabaseDescriptor.clientInitialization();

            CommitLogReader reader = new CommitLogReader();
            CommitLogDescriptor descriptor = CommitLogDescriptor.fromFileName(p.toFile().getName());
            System.out.println("Reading " + p.toFile().getAbsolutePath());
            reader.readCommitLogSegment(handler, p.toFile(), handler.getPosition(descriptor.id), CommitLogReader.ALL_MUTATIONS, false);
            if (reader.getInvalidMutations().isEmpty() && canDelete)
            {
                Files.delete(p);
            }
            else
            {
                for (Map.Entry<UUID, AtomicInteger> entry : reader.getInvalidMutations())
                {
                    boolean newCfid = !unknownCfids.contains(entry.getKey());
                    if (canReload && newCfid)
                    {
                        reloadSchema();
                        tryRead(p, canDelete, false);
                    }
                    else if (newCfid)
                    {
                        System.err.println("Unknown cfid: " + entry.getKey() + " value: " + entry.getValue());
                        unknownCfids.add(entry.getKey());
                    }
                }
            }
        }
        catch (RuntimeException e)
        {
            if (e.getCause() instanceof NoSuchFileException)
            {
                // This is a race between when we list the files and when we actually try to read them.
                // If we are in the commitlog directory, this is an expected condition when we are finishing up the processing the file.
                // This occurs outside of the try-catch that is supposed to catch this
            }
            else
            {
                throw e;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (Exception e)
        {
            System.err.println("exception! " + e.getMessage());
        }
    }

    private void readFolder(Path directory, boolean canDelete)
    {
        try
        {
            List<Future<?>> futures = new ArrayList<>();
            Stream<Path> files = Files.list(directory);
            DatabaseDescriptor.clientInitialization();

            files.forEach(p -> {
                futures.add(executor.submit(() -> {
                    tryRead(p, canDelete, true);
                }));
            });
            for (Future<?> future : futures)
            {
                future.get();
            }
        }
        catch (IOException | InterruptedException | ExecutionException e)
        {
            e.printStackTrace(System.err);
        }
    }

    private void readRaw()
    {
        readFolder(cdcRawDirectory, true);
    }

    private void readCurrent()
    {
        readFolder(commitlogDirectory, false);
    }

    private void iteration()
    {
        Future<?> overflow = executor.submit(this::readRaw);
        // Future<?> current = executor.submit(this::readCurrent);
        try
        {
            overflow.get();
            // current.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace(System.err);
        }
    }

    private synchronized void reloadSchema()
    {
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            for (CFMetaData cfm : Schema.instance.getTablesAndViews(keyspaceName))
            {
                Schema.instance.unload(cfm);
            }
        }
        new RemoteSchema(Cluster.builder()
                          .addContactPoint("localhost")
                          .build()).load();
    }

    public void start()
    {
        // Since CDC is only in newer Cassandras, we know how to read the schema values from it
        reloadSchema();
        executor.scheduleAtFixedRate(this::iteration, 0, 5000, TimeUnit.MILLISECONDS);
    }

    private interface CDCHandler extends CommitLogReadHandler
    {
        CommitLogPosition getPosition(long identifier);
    }

    private class PushToSnappy implements CDCHandler
    {
        private final Map<Long, Integer> furthestPosition = new HashMap<>();

        @Override
        public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
        {
            exception.printStackTrace(System.err);
            return false;
        }

        @Override
        public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
        {
            exception.printStackTrace(System.err);
        }

        @Override
        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            System.out.println("All: reading mutation " + m.toString(true));
            if (furthestPosition.getOrDefault(desc.id, 0) < entryLocation)
            {
//                boolean cdc = false;
//                for (UUID cfId : m.getColumnFamilyIds())
//                {
//                    if (Schema.instance.getCFMetaData(cfId).params.cdc)
//                        cdc = true;
//                }
//
//                if (cdc)
//                    System.out.println("reading mutation " + m.toString(true));
                String keyspace = m.getKeyspaceName();
                Collection<UUID> modifications = m.getColumnFamilyIds();

                List<String> cfnames = new ArrayList<>(modifications.size());
                for (UUID cfid : modifications)
                {
                    CFMetaData cfm = Schema.instance.getCFMetaData(cfid);
                    PartitionUpdate partitionUpdate = m.getPartitionUpdate(cfid);
                    for (Row row: partitionUpdate) {
                        String cols = "";
                        String colValues = "";

                        for (ColumnDefinition cd : row.columns()) {
                            if (cd.isComplex()) {
                                System.out.println("For key space " + keyspace + " for table name " + cfm.cfName
                                        + " for column " + cd.name.toCQLString()
                                        + " value " + row.getComplexColumnData(cd).toString());
                            } else {
                                if (keyspace.equalsIgnoreCase(cass_keyspace) && cfm.cfName.equalsIgnoreCase(cass_table)) {
                                    cols += ("," + cd.name.toCQLString() );
                                    if (cd.cellValueType() instanceof UTF8Type) {
                                        colValues += (",'" + cd.cellValueType().compose(row.getCell(cd).value()) + "'") ;
                                    } else {
                                        colValues += ("," + cd.cellValueType().compose(row.getCell(cd).value()));
                                    }


                                } else {
                                    System.out.println("For key space " + keyspace + " for table name " + cfm.cfName
                                            + " for column " + cd.name.toCQLString()
                                            + " value " + cd.cellValueType().compose(row.getCell(cd).value()));
                                }

                            }
                        }
                        List<ColumnDefinition> partCols = cfm.partitionKeyColumns();
                        for (ColumnDefinition cd: partCols)
                        {
                            if (keyspace.equalsIgnoreCase(cass_keyspace) && cfm.cfName.equalsIgnoreCase(cass_table)) {

                                cols += ("," + cd.name.toCQLString() );
                                if (cd.type instanceof UTF8Type) {
                                    Object s  = cd.type.compose(partitionUpdate.partitionKey().getKey());
                                    colValues += (", '" + s + "'");
                                } else if (cd.type instanceof Int32Type) {
                                    colValues += (", " + cd.type.compose(partitionUpdate.partitionKey().getKey()));
                                }
                            }
                        }

                        List<ColumnDefinition> clustCols = cfm.clusteringColumns();

                        for (int i = 0; i < clustCols.size(); i++)
                        {
                            if (keyspace.equalsIgnoreCase(cass_keyspace) && cfm.cfName.equalsIgnoreCase(cass_table)) {

                                ColumnDefinition c = clustCols.get(i);
                                cols += ("," + c.name.toCQLString() );
                                if (c.type instanceof UTF8Type) {
                                    Object s  = c.type.compose(row.clustering().getRawValues()[i]);
                                    colValues += (", '" + s + "'");
                                } else if (c.type instanceof Int32Type) {
                                    colValues += (", " + c.type.compose(row.clustering().getRawValues()[i]));
                                }
                            }
                        }

                        if (!cols.isEmpty()) {
                            String insertstmt = "Insert into snappy_" + cass_table + " (" +
                                    cols.substring(1) + ") values ( " + colValues.substring(1) + ");";
                            try {
                                System.out.println("Inserting row " + insertstmt);
                                conn.createStatement().execute(insertstmt);
                            } catch (SQLException e) {
                                System.err.print("Cannot execute the statement " + insertstmt);
                                e.printStackTrace();
                            }
                        }

                    }
                }
                System.out.println("Newer: reading mutation " + m.toString(true));
                furthestPosition.put(desc.id, entryLocation);
            }
        }

        @Override
        public CommitLogPosition getPosition(long identifier)
        {
            return Optional.ofNullable(furthestPosition.get(identifier))
                    .map(i -> new CommitLogPosition(identifier, i))
                    .orElse(CommitLogPosition.NONE);
        }
    }
}
