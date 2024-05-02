package batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FatalitiesByOperator {

    public static class OperatorMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Text operator = new Text();
        private LongWritable fatalities = new LongWritable();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip processing if the line contains the header
            if (line.startsWith("Date,Time,Location,Operator,Flight #,Route,Type,Registration,cn/In,Aboard,Fatalities,Ground,Summary")) {
                return;
            }

            // Split the line by comma, considering quoted values
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // Check if the line has the expected number of fields
            if (fields.length >= 11) {
                String operatorName = fields[3];
                System.out.println(operatorName+" "+fields[10]);
                fatalities.set(Long.parseLong(fields[10]));
                context.write(new Text(operatorName), fatalities);
            } else {
                // Log a warning for lines that don't match the expected format
                System.err.println("Invalid input format: " + line);
            }
        }
    }



    public static class OperatorFatalitiesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long totalFatalities = 0;

            // Sum up the fatalities count for this operator
            for (LongWritable value : values) {
                totalFatalities += value.get();
            }

            context.write(key, new LongWritable(totalFatalities));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Fatalities By Operator");

        job.setJarByClass(FatalitiesByOperator.class);
        job.setMapperClass(OperatorMapper.class);
        job.setReducerClass(OperatorFatalitiesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("src/main/resources/input/Airplane_Crashes_and_Fatalities.csv"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output7"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
