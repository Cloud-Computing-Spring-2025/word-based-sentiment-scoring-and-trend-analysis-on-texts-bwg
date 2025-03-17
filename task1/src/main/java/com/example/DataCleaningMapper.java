package com.example;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataCleaningMapper extends Mapper<Object, Text, Text, Text> {

    private static final List<String> STOPWORDS = Arrays.asList("the", "and", "is", "in", "to", "it", "that", "of", "with", "a", "on"); // Example stopwords

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming CSV format: bookID,title,year,content
        String[] parts = value.toString().split(",", -1); // Split by comma, -1 ensures empty fields are preserved

        if (parts.length >= 4) {
            String bookID = parts[0];    // Book ID
            String title = parts[1];     // Book Title
            String year = parts[2];      // Year of publication
            String content = parts[3];   // The content of the book

            // Clean the content: convert to lowercase, remove punctuation, stop words
            String cleanedContent = cleanText(content);

            // Emit (bookID, year) as key, cleaned content as value
            context.write(new Text(bookID + "," + year), new Text(cleanedContent));
        }
    }

    // Method to clean the text by lowercasing, removing punctuation, and removing stopwords
    private String cleanText(String text) {
        // Convert text to lowercase
        text = text.toLowerCase();

        // Remove punctuation
        text = text.replaceAll("[^a-zA-Z0-9\\s]", "");

        // Remove stopwords
        for (String stopword : STOPWORDS) {
            text = text.replaceAll("\\b" + stopword + "\\b", "");
        }

        return text;
    }
}
