package batch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FatalitiesByYear {

    public static class FatalitiesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable fatalities = new IntWritable();
        private Text year = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (fields.length >= 13) {
                String date = fields[0];
                String fatalitiesnumber = fields[10];
                System.out.println(" date : "+ date);
                if (!date.isEmpty()) {
                    String[] dateFields = date.split("/");
                    if (dateFields.length >= 3) {
                        String yearValue = dateFields[2];
                        year.set(yearValue);
                        int fatalitiesValue = Integer.parseInt(fatalitiesnumber);
                        fatalities.set(fatalitiesValue);
                        System.out.println(" year : "+yearValue+" fatalities: "+fatalitiesValue);
                        context.write(year, fatalities);
                    }
                } else {
                    // Log or skip records with missing or empty fields
                    System.out.println("Skipping record: " + line);
                }
            } else {
                // Log or handle records with insufficient fields
                System.out.println("Record has insufficient fields: " + line);
            }
        }

    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            // Output debug information
            System.out.println("Reducer Output: Key = " + key.toString() + ", Value = " + sum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "67108864"); // 64 MB
        Job job = Job.getInstance(conf, "Fatalities By Year");


        job.setJarByClass(FatalitiesByYear.class);
        job.setMapperClass(FatalitiesMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("src/main/resources/input/Airplane_Crashes_and_Fatalities.csv"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output6"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
