import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Trip {


 /* main method sets the mapper and reducer class with output  types with input and output paths */

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "NYC Yellow Taxi Trip Average Pricing per mile");
        job.setJarByClass(Trip.class);
        job.setMapperClass(Pr1_Mapper.class);
        job.setCombinerClass(Pr1_Reducer.class);
        job.setReducerClass(Pr1_Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
/* the mapper extracts the trip distance and fare and gives rounded distance and fare pair */
    public static class Pr1_Mapper extends Mapper<Object, Text, IntWritable, FloatWritable> {
        private IntWritable rn_trip_dist = new IntWritable();
        private FloatWritable trip_fare = new FloatWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dataParts = value.toString().split(",");
            if (dataParts.length > 5 && dataParts[0].matches("\\d+.*")) {
                try {
                    float distance = Float.parseFloat(dataParts[4]);
                    int rounded_dist = Math.round(distance);
                    float amount = Float.parseFloat(dataParts[dataParts.length - 1]);
                    if (rounded_dist >= 0 && rounded_dist < 200) {
                        rn_trip_dist.set(rounded_dist);
                        trip_fare.set(amount);
                        context.write(rn_trip_dist, trip_fare);
                    }
                } catch (NumberFormatException e) {
                    // Handling exception if parsing fails
                }
            }
        }
    }
/* calculates and gives the average fare for the rounded distance */
    public static class Pr1_Reducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
        private FloatWritable averageAmount = new FloatWritable();

        @Override
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float total = 0;
            int count = 0;
            for (FloatWritable val : values) {
                total += val.get();
                count++;
            }
            if (count > 0) {
                averageAmount.set(total / count);
                context.write(key, averageAmount);
            }
        }
    }
}