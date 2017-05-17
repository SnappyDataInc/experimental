package io.snappydata.examples;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Array;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import com.typesafe.config.Config;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Creates and loads Airline data from parquet files in row and column
 * tables. Also samples the data and stores it in a column table.
 *
 *
 * Run this on your local machine:
 * <p/>
 * `$ sbin/snappy-start-all.sh`
 * <p/>
 * `$ ./bin/snappy-job.sh submit --lead localhost:8090 \
 * --app-name JavaCreateAndLoadAirlineDataJob --class io.snappydata.examples.JavaCreateAndLoadAirlineDataJob \
 * --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar`
 */


public class JavaCreateAndLoadAirlineDataJob extends JavaSnappySQLJob {

  private String airlinefilePath = null;
  private String airlinereftablefilePath = null;
  private static final String colTable = "AIRLINE";
  private static final String rowTable = "AIRLINEREF";
  private static final String stagingAirline = "STAGING_AIRLINE";

  public static void main( String args[] ) {
      System.out.println("nothing to do");
  }
  
  @Override
  public Object runSnappyJob(SnappySession snc, Config jobConfig) {
	  
    try (PrintWriter pw = new PrintWriter("JavaCreateAndLoadAirlineDataJob.out")) {
      String currentDirectory = new File(".").getCanonicalPath();
      // Drop tables if already exists
      snc.dropTable(colTable, true);
      snc.dropTable(rowTable, true);
      snc.dropTable(stagingAirline, true);

      pw.println("****** JavaCreateAndLoadAirlineDataJob ******");

      // Create a DF from the parquet data file and make it a table
      Map<String, String> props = new HashMap<>();
      props.put("path", airlinefilePath);
      Dataset<Row> airlineDF = snc.catalog().createExternalTable(stagingAirline, "parquet", props);
      StructType updatedSchema = replaceReservedWords(airlineDF.schema());

      // Create a table in snappy store
      Map<String, String> columnTableProps = new HashMap<>();
      columnTableProps.put("buckets", "11");
      snc.createTable(colTable, "column",
          updatedSchema, columnTableProps, false);

      // Populate the table in snappy store
      airlineDF.write().mode(SaveMode.Append).saveAsTable(colTable);
      pw.println("Created and imported data in $colTable table.");

      // Create a DF from the airline ref data file
      Dataset<Row> airlinerefDF = snc.read().load(airlinereftablefilePath);

      // Create a table in snappy store
      snc.createTable(rowTable, "row", airlinerefDF.schema(),
          Collections.<String, String>emptyMap(), false);

      // Populate the table in snappy store
      airlinerefDF.write().mode(SaveMode.Append).saveAsTable(rowTable);

      pw.println("Created and imported data in $rowTable table");
      
      pw.println("Running a join query. Results: ");
      // Which airline out of SanFrancisco had most delays due to weather
      Row[] result = snc.sql("SELECT sum(WeatherDelay) totalWeatherDelay, "
      		+ "airlineref.DESCRIPTION FROM airline, airlineref "
      		+ "WHERE airline.UniqueCarrier = airlineref.CODE AND  "
      		+ "Origin like '%SFO%' AND "
      		+ "WeatherDelay > 0 "
      		+ "GROUP BY DESCRIPTION limit 0").collect();
      
      for(Row row: result) {
    	  pw.println(row);
      }
      
      pw.println("****** Job finished ******");
      return String.format("See %s/JavaCreateAndLoadAirlineDataJob.out",
          currentDirectory);
    } catch (IOException ioe) {
      StringWriter sw = new StringWriter();
      PrintWriter spw = new PrintWriter(sw);
      spw.println("ERROR: failed with " + ioe);
      ioe.printStackTrace(spw);
      return spw.toString();
    }
  }

  @Override
  public SnappyJobValidation isValidJob(SnappySession snc, Config config) {

    if (config.hasPath("airline_file")) {
      airlinefilePath = config.getString("airline_file");
    } else {
      airlinefilePath = "../../quickstart/data/airlineParquetData";
    }

    if (!(new File(airlinefilePath)).exists()) {
      return new SnappyJobInvalid("Incorrect airline path. " +
          "Specify airline_file property in APP_PROPS");
    }

    if (config.hasPath("airlineref_file")) {
      airlinereftablefilePath = config.getString("airlineref_file");
    } else {
      airlinereftablefilePath = "../../quickstart/data/airportcodeParquetData";
    }
    if (!(new File(airlinereftablefilePath)).exists()) {
      return new SnappyJobInvalid("Incorrect airline ref path. " +
          "Specify airlineref_file property in APP_PROPS");
    }

    return new SnappyJobValid();
  }

  private static StructType replaceReservedWords(StructType airlineSchema) {
    StructField[] fields = airlineSchema.fields();
    StructField[] newFields = new StructField[fields.length];
    for (StructField s : fields) {
      StructField newField;
      if (s.name().equals("Year")) {
        newField = new StructField("Year_", s.dataType(), s.nullable(), s.metadata());
      } else if (s.name().equals("Month")) {
        newField = new StructField("Month_", s.dataType(), s.nullable(), s.metadata());
      } else {
        newField = s;
      }
      newFields[airlineSchema.indexOf(s)] = newField;
    }
    return new StructType(newFields);
  }

}
