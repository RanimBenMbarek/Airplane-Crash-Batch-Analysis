package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class AccidentCountByYear {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "accident count by year");
        job.setJarByClass(AccidentCountByYear.class);
        job.setMapperClass(AccidentMapper.class);
        job.setCombinerClass(AccidentReducer.class);
        job.setReducerClass(AccidentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/input/Airplane_Crashes_and_Fatalities.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
