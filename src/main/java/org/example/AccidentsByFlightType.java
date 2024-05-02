package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable; // Import Serializable
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AccidentsByFlightType implements Serializable { // Implement Serializable

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: AccidentsByFlightType <input-file-path> <output-dir>");
            System.exit(1);
        }

        new AccidentsByFlightType().run(args[0], args[1]);
    }

    public void run(String inputFilePath, String outputDir) throws IOException {
        String hbaseTableName = "AccidentsByFlightType";
        String columnFamily = "Count";

        SparkConf conf = new SparkConf().setAppName(AccidentsByFlightType.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        // Count occurrences of each flight type
        JavaPairRDD<String, Integer> counts = textFile.flatMapToPair(line -> {
            String[] fields = splitCSVLine(line);
            if (fields.length >= 13) {
                String type = fields[6];
                if (!type.isEmpty()) {
                    return Arrays.asList(new Tuple2<>(type, 1)).iterator();
                }
            }
            return new ArrayList<Tuple2<String, Integer>>().iterator();
        }).reduceByKey(Integer::sum); // Aggregate counts by flight type

        // Save counts to HBase
        saveToHBase(counts.collect(), hbaseTableName, columnFamily);

        // Save results to output directory
        counts.saveAsTextFile(outputDir);

        // Close the Spark context
        sc.close();
    }

    private String[] splitCSVLine(String line) {
        boolean insideQuotes = false;
        StringBuilder fieldBuilder = new StringBuilder();
        List<String> fieldsList = new ArrayList<>();

        for (char c : line.toCharArray()) {
            if (c == ',' && !insideQuotes) {
                fieldsList.add(fieldBuilder.toString());
                fieldBuilder.setLength(0); // Clear the StringBuilder
            } else {
                if (c == '"') {
                    insideQuotes = !insideQuotes;
                }
                fieldBuilder.append(c);
            }
        }

        fieldsList.add(fieldBuilder.toString());

        return fieldsList.toArray(new String[0]);
    }

    private void saveToHBase(List<Tuple2<String, Integer>> data, String hbaseTableName, String columnFamily) throws IOException {
        Configuration hbaseConfig = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(hbaseTableName);
            if (!admin.tableExists(tableName)) {
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily))
                        .build();
                admin.createTable(tableDescriptor);
            }
            Table table = connection.getTable(tableName);
            for (Tuple2<String, Integer> entry : data) {
                try {
                    Put put = new Put(Bytes.toBytes(entry._1));
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("count"), Bytes.toBytes(entry._2.toString())); // Convert Integer to String
                    table.put(put);
                } catch (IOException e) {
                    // Log the error and continue processing other entries
                    System.err.println("Error putting data into HBase for key: " + entry._1 + ", Error: " + e.getMessage());
                }
            }
        }
    }
}
