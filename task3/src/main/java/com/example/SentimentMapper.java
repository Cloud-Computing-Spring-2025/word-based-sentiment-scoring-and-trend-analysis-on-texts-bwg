package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<String, Integer> sentimentLexicon = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        // Load the AFINN-111 sentiment lexicon from HDFS
        Configuration conf = context.getConfiguration();
        Path lexiconPath = new Path("/input/dataset/afinn.txt"); // HDFS path to the afinn.txt file
        FileSystem fs = FileSystem.get(conf);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(lexiconPath)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String word = parts[0].toLowerCase();
                    int score = Integer.parseInt(parts[1]);
                    sentimentLexicon.put(word, score);
                }
            }
        }
        System.out.println("Loaded AFINN-111 lexicon with " + sentimentLexicon.size() + " entries.");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length < 4) {
            System.err.println("Invalid input line: " + value.toString());
            return; // Skip invalid lines
        }

        String bookID = parts[0];
        String word = parts[1].toLowerCase().replaceAll("[^a-z]", ""); // Normalize word
        String year = parts[2];
        String rawFrequency = parts[3].trim(); // Trim whitespace from the frequency field

        int frequency;
        try {
            // Sanitize the frequency field by removing non-numeric characters
            rawFrequency = rawFrequency.replaceAll("[^0-9]", "");
            frequency = Integer.parseInt(rawFrequency);
        } catch (NumberFormatException e) {
            System.err.println("Invalid frequency: " + rawFrequency + " in line: " + value.toString());
            return; // Skip lines with invalid frequency
        }

        // Check if the word exists in the sentiment lexicon
        if (sentimentLexicon.containsKey(word)) {
            int sentimentScore = sentimentLexicon.get(word) * frequency;
            System.out.println("Mapper emitting: " + bookID + "," + year + " -> " + sentimentScore);
            context.write(new Text(bookID + "," + year), new IntWritable(sentimentScore)); // Emit (bookID, year) -> sentimentScore
        } else {
            System.out.println("Word not found in lexicon: " + word);
        }
    }
}
