import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FatalitiesRatioByYear {

    public static class FatalitiesMapper extends Mapper<Object, Text, Text, Text> {

        private Text year = new Text();
        private Text fatalitiesTotalPeople = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = splitCSVLine(line);

            if (fields.length >= 10) {
                String date = fields[0];
                if (!date.isEmpty()) {
                    String[] dateFields = date.split("/");
                    if (dateFields.length >= 3) {
                        String yearValue = dateFields[2];
                        year.set(yearValue);
                        fatalitiesTotalPeople.set(fields[10] + "," + fields[9]); // Concatenate fatalities and total people
                        context.write(year, fatalitiesTotalPeople);
                    }
                } else {
                    System.out.println("Invalid date format: " + date);
                }
            } else {
                System.out.println("Unexpected input format: " + line);
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

    public static class RatioReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalFatalities = 0;
            int totalPeople = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                totalFatalities += Integer.parseInt(parts[0]);
                totalPeople += Integer.parseInt(parts[1]);
            }

            double ratio = totalPeople > 0 ? (double) totalFatalities / totalPeople : 0.0;
            String formattedRatio = String.format("%.2f", ratio);
            context.write(key, new Text(formattedRatio));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Fatalities Ratio By Year");

        job.setJarByClass(FatalitiesRatioByYear.class);
        job.setMapperClass(FatalitiesMapper.class);
        job.setReducerClass(RatioReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("src/main/resources/input/Airplane_Crashes_and_Fatalities.csv"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output5"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
