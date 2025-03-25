package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.SentimentMapper;
import com.example.SentimentReducer;

public class SentimentAnalysisDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SentimentAnalysisDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create a new Hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentiment Analysis");

        // Set the Jar file
        job.setJarByClass(SentimentAnalysisDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output paths from arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
