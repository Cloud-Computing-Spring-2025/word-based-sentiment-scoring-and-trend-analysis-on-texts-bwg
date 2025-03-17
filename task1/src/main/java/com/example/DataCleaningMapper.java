package com.example;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;

public class DataCleaningMapper extends Mapper<LongWritable, Text, Text, Text> {
    private boolean isFirstLine = true;  // Skip header
    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        "the", "and", "is", "in", "to", "of", "for", "on", "that", "this", "with", "as", "was", "by", "his", "he", "she", "it", "at", "or", "an", "be", "from"
    ));

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip header
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        // Try to split the CSV line safely
        String[] fields = line.split(",", 4);  // Split only on the first 3 commas

        if (fields.length < 4) {
            return; // Skip malformed lines
        }

        String bookID = fields[0].trim();
        String year = fields[2].trim();
        String content = fields[3].toLowerCase().replaceAll("[^a-z ]", ""); // Remove punctuation
        System.out.println("DEBUG: Parsed -> BookID: " + fields[0] + ", Year: " + fields[2] + ", Content: " + fields[3]);
        // Tokenize and remove stopwords
        StringTokenizer tokenizer = new StringTokenizer(content);
        List<String> cleanedWords = new ArrayList<>();

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();
            if (!STOPWORDS.contains(token) && token.length() > 1) {
                cleanedWords.add(token);
            }
        }

        // Emit cleaned text **without breaking multi-line content**
        if (!cleanedWords.isEmpty()) {
            context.write(new Text(bookID + "," + year), new Text(String.join(" ", cleanedWords)));
        }
    }
}
