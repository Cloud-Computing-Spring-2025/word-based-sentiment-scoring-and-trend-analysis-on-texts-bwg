package com.example;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text wordKey = new Text();
    private static final Map<String, String> LEMMATIZATION_DICT = new HashMap<>();

    static {
        loadLemmatizationData(); // Load from file
    }

    private static void loadLemmatizationData() {
        try (BufferedReader reader = new BufferedReader(new FileReader("lemmatization.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length == 2) {
                    LEMMATIZATION_DICT.put(parts[0], parts[1]);
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading lemmatization data: " + e.getMessage());
        }
    }

    private String lemmatize(String word) {
        word = word.toLowerCase();
        if (LEMMATIZATION_DICT.containsKey(word)) {
            return LEMMATIZATION_DICT.get(word);
        }
        if (word.endsWith("ing") && word.length() > 4) {
            return word.substring(0, word.length() - 3);
        }
        if (word.endsWith("ed") && word.length() > 3) {
            return word.substring(0, word.length() - 2);
        }
        if (word.endsWith("ies") && word.length() > 3) {
            return word.substring(0, word.length() - 3) + "y";
        }
        if (word.endsWith("es") && word.length() > 3) {
            return word.substring(0, word.length() - 2);
        }
        if (word.endsWith("s") && word.length() > 3) {
            return word.substring(0, word.length() - 1);
        }
        return word;
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t", 2);
        if (parts.length < 2) return;

        String[] bookInfo = parts[0].split(",");
        if (bookInfo.length < 2) return;

        String bookID = bookInfo[0].trim();
        String year = bookInfo[1].trim();
        String sentence = parts[1].trim().replaceAll("[^a-zA-Z ]", "").toLowerCase();

        StringTokenizer tokenizer = new StringTokenizer(sentence);
        while (tokenizer.hasMoreTokens()) {
            String lemma = lemmatize(tokenizer.nextToken());
            String keyOut = bookID + "," + lemma + "," + year;
            wordKey.set(keyOut);
            context.write(wordKey, one);
        }
    }
}
