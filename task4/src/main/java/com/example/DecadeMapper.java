package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DecadeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text decadeKey = new Text();
    private IntWritable countValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line: "1,able,1852    1"
        String[] parts = value.toString().split("\t"); // Split by tab first
        if (parts.length != 2) return; // Ignore malformed lines

        String[] firstParts = parts[0].split(","); // Split first part by commas
        if (firstParts.length != 3) return;

        String bookID = firstParts[0]; 
        String word = firstParts[1];
        int year;

        try {
            year = Integer.parseInt(firstParts[2]); // Parse year
        } catch (NumberFormatException e) {
            return; // Ignore invalid years
        }

        int decade = (year / 10) * 10; // Convert year to decade
        int count = Integer.parseInt(parts[1]); // Parse count

        decadeKey.set(bookID + "," + decade); // Use (bookID, decade) as key
        countValue.set(count);

        context.write(decadeKey, countValue);
    }
}
