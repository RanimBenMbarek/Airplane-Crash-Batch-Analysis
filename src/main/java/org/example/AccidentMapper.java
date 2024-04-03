package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccidentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text year = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(","); // Assuming comma-separated values

        // Extract year from the Date field
        String date = fields[0];
        String[] dateParts = date.split("/");
        if (dateParts.length >= 3) {
            year.set(dateParts[2]);
            context.write(year, one);
        }
    }
}
