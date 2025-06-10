package org.name.application;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.opencsv.CSVReader;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.annotations.CommandLineArguments;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.name.application.interfaces.TablesInitService;
import org.name.domain.CoordinateTuple;
import org.name.domain.TableCell;
import org.name.domain.TableRow;
import org.name.domain.repositories.TableRepository;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;


@ApplicationScoped
public class TableInitServiceImpl implements TablesInitService {

    @Inject
    EntityManager eM;

    @ConfigProperty(name = "tables.dir")
    String tablesDir;

    @Inject
    @CommandLineArguments
    String[] args;

    @Inject
    TableRepository tableRepository;

    //not used
    //@RestClient
    //GenesisApiClient genesisApiClient;

    SparkSession sparkSession;
    @ConfigProperty(name = "enable-table-init", defaultValue = "false")
    boolean enableInit;

    @ConfigProperty(name = "enable-export-tables", defaultValue = "false")
    boolean enableExport;
    void onStart(@Observes StartupEvent ev) {
        Log.info("Java version: " + System.getProperty("java.version"));
        Log.info("Classpath: " + System.getProperty("java.class.path"));

        if (Arrays.asList(args).contains("--enable-table-init")) {
            initTables();
        }
        if (Arrays.asList(args).contains("--enable-export-tables")) {
            Log.info("write tables");
            //writeTables();
        }
//        sparkSession = getSparkSession();
//        if (sparkSession != null) {
            //writeTables();
//        } else {
//            Log.error("Failed to create Spark session, aborting writeTables.");
//        }
    }
//
//    public SparkSession getSparkSession() {
//        sparkSession.close();
//        try {
//                sparkSession= SparkSession.builder()
//                        .appName("Quarkus Spark Example")
//                        .master("local[*]")
//                       // .config("spark.driver.memory", "4g") // Specify driver memory
//                       // .config("spark.executor.memory", "4g") // Specify executor memory
//                        .getOrCreate();
//
//                Log.info("Spark session created successfully.");
//            } catch (Exception e) {
//                Log.error("Error creating Spark session: " + e.getMessage(), e);
//            }
//        return sparkSession;
//    }

    Map<String, Integer> annotatorIndex = new HashMap<>();

    //currently not needed, as position is always zero -> no connected questions
    //Map<String, Integer> positionIndex;


    //method to write all annotated tables from the database as csv
//    private void writeTables(){
//        sparkSession.sparkContext().setLogLevel("DEBUG");
//        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO midd1 tabletest creating!");
//        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO started tabletest creating!");
//        List<TableRow> tableRows = eM.createQuery(
//                "FROM TableRow WHERE isAnswered = true", TableRow.class).getResultList();
//        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO midd0 tabletest creating!");
//
//        Dataset<Row> tableRowsdf = sparkSession.createDataFrame(tableRows, TableRow.class);
//        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO midd2 tabletest creating!");
//        //register annotator udf
//        getSparkSession().udf().register("getAnnotator", udf((String id) -> annotatorIndex.get(id) == null ? annotatorIndex.put(id, 0)
//                : annotatorIndex.put(id, annotatorIndex.get(id) + 1), DataTypes.StringType));
//        getSparkSession().udf().register("getAnswerCoordinates", udf((Integer row, Integer column) ->
//                        Arrays.asList(new CoordinateTuple(row, column).toString()), DataTypes.createArrayType(
//                        DataTypes.StringType, false
//                        )
//                )
//        );
//        getSparkSession().udf().register("getAnswerText", udf((Map<String, String> rowEntries, Integer answerIndex) ->
//                                Arrays.asList(rowEntries.get(rowEntries.keySet().toArray()[answerIndex])), DataTypes.createArrayType(
//                                DataTypes.StringType, false
//                        )
//                )
//        );
//
//
//        tableRowsdf
//                .withColumn("annotator", callUDF("getAnnotator", col("id")))
//                .withColumn("position", lit(0))
//                .withColumnRenamed("tableId", "id")
//                .withColumn("answer_coordinates", callUDF("getAnswerColumns", sequence(col("row"), col("answerColumn"))))
//                .withColumn("answer_text", callUDF("getAnswerText", sequence(col("rowEntries"), col("answerColumn"))))
//                .select(col("id"), col("annotator"), col("position"), col("question"), col("id").as("table_file"),
//                        col("answer_coordinates"))
//                .write().csv("tabletest1.csv");
//        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO tabletest made!");
//        sparkSession.stop();
//    }

    private void writeTables() {
        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO started tabletest creating!");

        // Load table ID to filename mapping from data.csv
        Map<String, String> tableFileMapping = loadTableFileMapping("data.csv");

        // Fetch answered TableRow records
        List<TableRow> tableRows = eM.createQuery(
                "FROM TableRow WHERE isAnswered = true", TableRow.class).getResultList();
        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO tabletest creating!");

        // Transform rows for CSV
        List<Map<String, Object>> transformedRows = tableRows.stream().map(row -> {
            Map<String, Object> transformedRow = new HashMap<>();

            transformedRow.put("id", row.tableId());
            transformedRow.put("annotator", getAnnotator(row.tableId()));
            transformedRow.put("position", 0);  // Position is set to 0 as per the original logic
            transformedRow.put("question", row.question());
            transformedRow.put("table_file", tableFileMapping.getOrDefault(row.tableId(), "UNKNOWN")); // Use the map here

            // Format answer_coordinates as a list with tuple format ['(row,column)']
            transformedRow.put("answer_coordinates", "[('" + row.row() + "," + row.answerColumn() + "')]");

            // Add actual answer text as a String by retrieving from TableCell
            transformedRow.put("answer_text", getAnswerText(row.rowEntries(), row.answerColumn()));
            return transformedRow;
        }).collect(Collectors.toList());

        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO tabletest made!");

        writeCsv("tabletest.csv", transformedRows);
    }

    private Map<String, String> loadTableFileMapping(String csvFilePath) {
        Map<String, String> mapping = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            // Skip header
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(";");
                if (parts.length >= 4) {
                    String code = parts[1].trim();  // tableId
                    String filename = parts[3].trim();  // filename
                    mapping.put(code, filename);
                }
            }
        } catch (IOException e) {
            Log.error("Error reading CSV file: " + e.getMessage());
        }
        return mapping;
    }

    private String getAnnotator(String id) {
        if (annotatorIndex.containsKey(id)) {
            // If the key exists, increment the value and return the previous value
            int currentValue = annotatorIndex.get(id);
            annotatorIndex.put(id, currentValue + 1);
            return String.valueOf(currentValue);
        } else {
            // If the key does not exist, initialize it with 0 and return 0
            annotatorIndex.put(id, 1); // Prepare for the next call
            return "0";
        }
    }


    private String getAnswerCoordinates(Integer row, Integer column) {
        return "[('" + row + "," + column + "')]";  // Modified for correct format
    }

    private String getAnswerText(Set<TableCell> rowEntries, Integer answerColumn) {
        // Iterate over TableCell entries to find the one matching answerColumn
        for (TableCell cell : rowEntries) {
            if (cell.getColumnId().equals(answerColumn)) {
                return cell.getCellValue();  // Return the cell value if column matches
            }
        }
        return "";  // Return empty string if no match is found
    }

    private void writeCsv(String filePath, List<Map<String, Object>> rows) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("id,annotator,position,question,table_file,answer_coordinates,answer_text\n"); // Header

            for (Map<String, Object> row : rows) {
                writer.write(
                        row.get("id") + "," +
                                row.get("annotator") + "," +
                                row.get("position") + "," +
                                "\"" + row.get("question") + "\"," +  // Wrap question in quotes to handle commas
                                row.get("table_file") + "," +
                                "\"" + row.get("answer_coordinates") + "\"," +  // Wrap coordinates in quotes for format consistency
                                "\"" + row.get("answer_text") + "\""  // Wrap answer_text in quotes for format consistency
                );
                writer.newLine();
            }
            Log.info("CSV file written to " + filePath);
        } catch (IOException e) {
            Log.error("Error writing CSV file: " + e.getMessage());
        }
    }




    @Override
    public void initTables() {
        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO Started table extraction and initialization process!");
        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO Extracting csvs from /tables/...");
        List<List<TableRow>> tableList = readTablesFromSource();
        int i = 1;
        int size = tableList.size();
        for (List<TableRow> table : tableList) {
            Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO Inserting table " + i + "/" + size + " into database.");
            tableRepository.insertTable(table);
            ++i;
        }
        Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO Ended table extraction and initialization process!");
    }


    private List<List<TableRow>> readTablesFromSource() {
        List<List<TableRow>> CSVTableList = new ArrayList<>();
        MappingIterator<TableMetaData> tableMetaDataIter = null;
        try {
            CsvMapper mapper = new CsvMapper();
            CsvSchema schema = CsvSchema.builder()
                    .setColumnSeparator(';')
                    .setUseHeader(true)
                    .setStrictHeaders(true)
                    .setSkipFirstDataRow(true)
                    .addColumn("", CsvSchema.ColumnType.NUMBER)
                    .addColumn("code", CsvSchema.ColumnType.STRING)
                    .addColumn("content", CsvSchema.ColumnType.STRING)
                    .addColumn("filename", CsvSchema.ColumnType.STRING)
                    .build();
            tableMetaDataIter = mapper.reader(TableMetaData.class).with(schema).readValues(new File("data.csv"));
            List<TableMetaData> tableMetaDataList = tableMetaDataIter.readAll();
            for(TableMetaData tableMetaData : tableMetaDataList){
                Log.info("\n" + new Timestamp(System.currentTimeMillis()) + " INFO Parsing table " + tableMetaData.getTableId() + "...");
                FileReader fileReader = new FileReader(tableMetaData.getFilePath());
                CSVReader csvReader = new CSVReader(fileReader, ';');
                CSVTableList.add(parseCSVToTable(tableMetaData.getTableId(), csvReader.readAll(), tableMetaData.getContent()));
            }
        } catch (IOException e) {
            return CSVTableList;
        }


        return CSVTableList;
    }

    private List<TableRow> parseCSVToTable(String tableName, List<String[]> csv, String content) {
        List<TableRow> table = new ArrayList<>();
        List<String> headerNames = Arrays.stream(csv.get(0)).collect(Collectors.toList());
        csv.remove(csv.get(0));

        int i = 0;
        for (String[] row : csv){
            table.add(TableRow.create(row, headerNames, tableName, i, content));
            ++i;
        }

        return table;
    }
}
