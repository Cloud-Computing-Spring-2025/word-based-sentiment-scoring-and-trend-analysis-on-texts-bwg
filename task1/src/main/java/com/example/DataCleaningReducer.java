package com.example;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataCleaningReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Extract bookID and year from the key
        
        String[] keyParts = key.toString().split(",");
        if (keyParts.length < 2) {
            System.err.println("Invalid key format: " + key.toString());
            return;  // Skip this record if the key is invalid
        }

        String bookID = keyParts[0];
        String year = keyParts[1];


        // Initialize a variable for the aggregated text
        StringBuilder aggregatedText = new StringBuilder();

        // Aggregate the text for this book (if split into multiple lines)
        for (Text value : values) {
            aggregatedText.append(value.toString()).append(" ");
        }

        // Output the cleaned data with bookID, title, year, and the aggregated cleaned text
        context.write(new Text(bookID + "," + year), new Text(aggregatedText.toString().trim()));
    }
}
