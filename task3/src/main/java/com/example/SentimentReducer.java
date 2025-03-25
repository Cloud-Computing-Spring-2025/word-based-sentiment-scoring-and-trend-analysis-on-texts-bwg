package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;

public class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalSentiment = 0;

        // Iterate through all the sentiment scores for a specific (bookID, year)
        for (IntWritable val : values) {
            totalSentiment += val.get();
        }

        System.out.println("Reducer emitting: " + key.toString() + " -> " + totalSentiment);

        // Emit the result: (bookID, year) -> totalSentiment
        context.write(key, new IntWritable(totalSentiment));
    }
}
