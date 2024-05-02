package batch.ByYear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccidentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text yearMonth = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 1) {
            String date = fields[0];
            String[] dateFields = date.split("/");
            if (dateFields.length >= 2) {
                String year = dateFields[2];
                String month = dateFields[0];
                yearMonth.set(year + "-" + month);
                context.write(yearMonth, one);
            }
        }
    }
}
