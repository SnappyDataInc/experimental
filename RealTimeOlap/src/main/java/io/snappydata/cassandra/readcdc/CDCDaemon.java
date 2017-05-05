package io.snappydata.cassandra.readcdc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import com.datastax.driver.core.Cluster;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;

public class CDCDaemon
{
    public static final String SCHEMA_NAME = "system_schema";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    private final Path commitlogDirectory = Paths.get("/home/hemant/install/apache-cassandra-3.10/data/commitlog");
    private final Path cdcRawDirectory = Paths.get("/home/hemant/install/apache-cassandra-3.10/data/cdc_raw");
    private final CDCHandler handler = new SimpleCount(); //new LogHandler1();
    private final Set<UUID> unknownCfids = Sets.newConcurrentHashSet();

    private CDCDaemon()
    {
        Config.setClientMode(true);
    }

    public static void main(String[] args)
    {
        new CDCDaemon().start();
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
    public class LogHandler1 implements CDCHandler
    {
        public List<Mutation> seenMutations = new ArrayList<Mutation>();
        public boolean sawStopOnErrorCheck = false;

        private final CFMetaData cfm;
        @Override
        public CommitLogPosition getPosition(long identifier)
        {
            return CommitLogPosition.NONE;
        }
        // Accept all
        public LogHandler1()
        {
            this.cfm = null;
        }

        public LogHandler1(CFMetaData cfm)
        {
            this.cfm = cfm;
        }

        public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
        {
            sawStopOnErrorCheck = true;
            return false;
        }

        public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
        {
            sawStopOnErrorCheck = true;
        }

        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            count++;
            if (!m.getKeyspaceName().startsWith("system")) {
                System.out.println("Mutation " + m.toString(true));
                seenMutations.add(m);
            }
        }
        public int count = 0;
        public int seenMutationCount() { return seenMutations.size(); }
    }

    private static class SimpleCount implements CDCHandler
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
                        System.out.println("Starting a row update");
                        for (ColumnDefinition cd : row.columns()) {
                            if (cd.isComplex()) {
                                System.out.println("For key space " + keyspace + " for table name " + cfm.cfName
                                        + " for column " + cd.name.toCQLString()
                                        + " value " + row.getComplexColumnData(cd).toString());
                            } else {
                                System.out.println("For key space " + keyspace + " for table name " + cfm.cfName
                                        + " for column " + cd.name.toCQLString()
                                        + " value " + cd.cellValueType().compose(row.getCell(cd).value()));
                            }
                        }
                        System.out.println("Ending a row update");
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
