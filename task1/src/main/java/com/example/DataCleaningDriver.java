package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCleaningDriver {

    public static void main(String[] args) throws Exception {
        // Ensure proper usage
        if (args.length != 2) {
            System.out.println("Arguments: ");
            for (String arg : args) {
                System.out.println(arg);
            }
            System.err.println("Usage: DataCleaningDriver <input path> <output path>");
            System.exit(-1);
        }
        

        // Create a new Hadoop configuration
        Configuration conf = new Configuration();

        // Create a new MapReduce job
        Job job = Job.getInstance(conf, "Data Extraction and Cleaning");

        // Set the Jar file and Mapper, Reducer classes
        job.setJarByClass(DataCleaningDriver.class);
        job.setMapperClass(DataCleaningMapper.class);
        job.setReducerClass(DataCleaningReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input directory (CSV files)
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory (Cleaned data)

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
