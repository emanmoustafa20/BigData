package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException{
        if (args.length != 2){
            System.out.println("Usage WordCount <input-file> <output-dir>");
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "City Avg - CIT650");
        job.setJarByClass(Main.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        }
    public static class MapperClass extends Mapper<Object, Text, Text, FloatWritable> {

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context)
                throws IOException, InterruptedException {


            String line = value.toString();
            String[] parts = line.split(",");

            if (parts.length >= 2) {
                String city = parts[0];
                float tempInFahrenheit;
                try {
                    tempInFahrenheit = Float.parseFloat(parts[1]);
                } catch (NumberFormatException e) {
                    // Handle invalid number format
                    return;
                }

                float tempInCelsius = convertToCelsius(tempInFahrenheit);
                context.write(new Text(city), new FloatWritable(tempInCelsius));
            }
        }

        private float convertToCelsius(float fahrenheit) {
            return (fahrenheit - 32) * 5 / 9.0f;
        }

    }

    public static class ReducerClass extends Reducer<Text, FloatWritable, Text, FloatWritable>{

        private final FloatWritable result = new FloatWritable();
        float average;

        @Override
            protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {

            // key: word
            // values: list of ones
            // context: to send the final results

            // Container to sum the ones of the same key
            float sum = 0;
            int count =0;
            // Summing the ones from the list of values
            for (FloatWritable floatWritable : values){
                sum += floatWritable.get();
                count++;
            }

            average = sum/count;
            // Setting the sum result to IntWritable
            result.set(average);
            // Emitting the final result as a tuple of (word, count)
            context.write(key, result);

        }
    }

}
