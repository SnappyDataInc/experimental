package io.snappydata.cassandra.readcdc;

import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.schema.*;

/**
 * This class allows us to read a schema from a running Cassandra instance, and build a local Schema instance from it.
 */
public class RemoteSchema
{
    final Cluster cluster;
    final Session session;

    public RemoteSchema(Cluster cluster)
    {
        this.cluster = cluster;
        session = cluster.connect(CDCDaemon.SCHEMA_NAME);
    }

    private CFMetaData.DroppedColumn resolveDropped(Row droppedColumn)
    {
        String keyspaceName = droppedColumn.getString("keyspace_name");
        String columnName = droppedColumn.getString("column_name");
        String type = droppedColumn.getString("type");
        long dropped = droppedColumn.getTime("dropped_time");
        return new CFMetaData.DroppedColumn(columnName,
                                            CQLTypeParser.parse(keyspaceName, type, Types.none()),
                                            dropped);
    }

    private ColumnDefinition resolveColumn(Row column)
    {
        String keyspaceName = column.getString("keyspace_name");
        String tableName = column.getString("table_name");
        ColumnDefinition.Kind kind = ColumnDefinition.Kind.valueOf(column.getString("kind").toUpperCase());
        int position = column.getInt("position");
        String columnName = column.getString("column_name");
        ByteBuffer columnNameBytes = column.getBytes("column_name_bytes");
        String type = column.getString("type");
        return new ColumnDefinition(keyspaceName,
                                    tableName,
                                    ColumnIdentifier.getInterned(CQLTypeParser.parse(keyspaceName, type, Types.none()),
                                            columnNameBytes, columnName),
                                    CQLTypeParser.parse(keyspaceName, type, Types.none()),
                                    position,
                                    kind);
    }

    private CFMetaData resolveCF(Row table)
    {
        String keyspaceName = table.getString("keyspace_name");
        String tableName = table.getString("table_name");
        UUID cfId = table.getUUID("id");
        EnumSet<CFMetaData.Flag> flags = EnumSet.copyOf(CFMetaData.flagsFromStrings(table.getSet("flags", String.class)));
        List<ColumnDefinition> columns = new ArrayList<>();
        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns = new HashMap<>();

        {
            List<Row> rows = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", CDCDaemon.SCHEMA_NAME, SchemaKeyspace.COLUMNS),
             keyspaceName,
             tableName)
                                 .all();
            for (Row row : rows)
            {
                columns.add(resolveColumn(row));
            }
        }

        {
            List<Row> rows = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", CDCDaemon.SCHEMA_NAME, SchemaKeyspace.DROPPED_COLUMNS),
             keyspaceName,
             tableName)
                              .all();
            for (Row row : rows)
            {
                droppedColumns.put(row.getBytes("column_name"), resolveDropped(row));
            }
        }

        return CFMetaData.create(keyspaceName,
         tableName,
         cfId,
         flags.contains(CFMetaData.Flag.DENSE),
         flags.contains(CFMetaData.Flag.COMPOUND),
         flags.contains(CFMetaData.Flag.SUPER),
         flags.contains(CFMetaData.Flag.COUNTER),
         false,
         columns,
         new RandomPartitioner())
        .droppedColumns(droppedColumns)
         .params(TableParams.builder().cdc(table.getBool("cdc")).build());
    }

    private KeyspaceMetadata resolveKeyspace(Row keyspace)
    {
        String keyspaceName = keyspace.getString("keyspace_name");
        List<Row> tables = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", CDCDaemon.SCHEMA_NAME, SchemaKeyspace.TABLES),
         keyspaceName).all();
        Tables.Builder builder = Tables.builder();
        for (Row row : tables)
        {
            builder.add(resolveCF(row));
        }
        return KeyspaceMetadata.create(keyspaceName,
         KeyspaceParams.create(keyspace.getBool("durable_writes"),
          keyspace.getMap("replication", String.class, String.class)),
         builder.build());
    }

    public void load()
    {
        List<Row> keyspaces = session.execute(String.format("SELECT * FROM %s.%s", CDCDaemon.SCHEMA_NAME, SchemaKeyspace.KEYSPACES))
                               .all();
        for (Row row : keyspaces)
        {
            KeyspaceMetadata keyspace = resolveKeyspace(row);
            Schema.instance.load(keyspace);
        }
    }
}
