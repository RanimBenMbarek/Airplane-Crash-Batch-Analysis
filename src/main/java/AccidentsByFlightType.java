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

public class AccidentsByFlightType {

    public static class FlightTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text flightType = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = splitCSVLine(line);
            if (fields.length >= 13) {
                String type = fields[6];
                if (!type.isEmpty()) {
                    flightType.set(type);
                    context.write(flightType, one); // Emitting (flight type, 1)

                } else {
                    // Log or skip records with missing or empty fields
                    System.out.println("Skipping record: " + line);
                }
            } else {
                // Log or handle records with insufficient fields
                System.out.println("Record has insufficient fields: " + line);
            }
        }
        private String[] splitCSVLine(String line) {
            boolean insideQuotes = false;
            StringBuilder fieldBuilder = new StringBuilder();
            java.util.List<String> fieldsList = new java.util.ArrayList<>();

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
    }



    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "accidents by flight type");

        job.setJarByClass(AccidentsByFlightType.class);
        job.setMapperClass(FlightTypeMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("src/main/resources/input/Airplane_Crashes_and_Fatalities.csv"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output3"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

