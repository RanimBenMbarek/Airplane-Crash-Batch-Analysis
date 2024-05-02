package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FatalitiesRatioByYear {
    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.err.println("Usage: FatalitiesRatioByYear <input-file-path> <output-dir>");
            System.exit(1);
        }
        String inputFilePath = args[0];
        String outputDir = args[1];

        new FatalitiesRatioByYear().run(inputFilePath, outputDir);
    }

    public void run(String inputFilePath, String outputDir) throws IOException {
        SparkConf conf = new SparkConf().setAppName(FatalitiesRatioByYear.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        // Mapper Logic
        JavaPairRDD<String, Tuple2<Integer, Integer>> yearFatalitiesTotalPeople = textFile.flatMapToPair(line -> {
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            List<Tuple2<String, Tuple2<Integer, Integer>>> result = new ArrayList<>();
            if (fields.length >= 10 && !fields[9].isEmpty() && !fields[10].isEmpty()) {
                String date = fields[0];
                if (!date.isEmpty()) {
                    String[] dateFields = date.split("/");
                    if (dateFields.length >= 3) {
                        String yearValue = dateFields[2];
                        int fatalities = Integer.parseInt(fields[10]);
                        int totalPeople = Integer.parseInt(fields[9]);
                        result.add(new Tuple2<>(yearValue, new Tuple2<>(fatalities, totalPeople)));
                    }
                }
            }
            return result.iterator();
        });

        // Reducer Logic
        JavaPairRDD<String, Tuple2<Integer, Integer>> aggregatedYearData = yearFatalitiesTotalPeople
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        JavaPairRDD<String, Double> fatalitiesRatioByYear = aggregatedYearData.mapValues(data -> {
            double ratio = (double) data._1 / data._2;
            return ratio;
        });

        // Save result to HBase
        saveToHBase(fatalitiesRatioByYear.collect(), "FatalitiesRatioByYear", "Ratio");

        // Save results to output directory
        fatalitiesRatioByYear.saveAsTextFile(outputDir);

        // Close the Spark context
        sc.close();
    }

    private void saveToHBase(List<Tuple2<String, Double>> data, String hbaseTableName, String columnFamily) throws IOException {
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
            for (Tuple2<String, Double> entry : data) {
                try {
                    Put put = new Put(Bytes.toBytes(entry._1));
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ratio"), Bytes.toBytes(Double.toString(entry._2)));
                    table.put(put);
                } catch (IOException e) {
                    // Log the error and continue processing other entries
                    System.err.println("Error putting data into HBase for key: " + entry._1 + ", Error: " + e.getMessage());
                }
            }
        }
    }

}
