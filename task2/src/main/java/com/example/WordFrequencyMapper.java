package com.example;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.stanford.nlp.simple.Sentence;

public class WordFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text wordKey = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Expected input format: bookID,year    sentence
        String[] parts = value.toString().split("\t", 2);
        if (parts.length < 2) {
            return; // Skip malformed lines
        }

        String[] bookInfo = parts[0].split(",");
        if (bookInfo.length < 2) {
            return; // Skip malformed lines
        }

        String bookID = bookInfo[0].trim();
        String year = bookInfo[1].trim();
        String sentence = parts[1].trim();

        // Tokenization and Lemmatization using Stanford NLP Simple API
        Sentence sent = new Sentence(sentence);
        for (String lemma : sent.lemmas()) {
            String keyOut = bookID + "," + lemma.toLowerCase() + "," + year;
            wordKey.set(keyOut);
            context.write(wordKey, one);
        }
    }
}
