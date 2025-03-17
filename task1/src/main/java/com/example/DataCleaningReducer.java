package com.example;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class DataCleaningReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder aggregatedText = new StringBuilder();

        for (Text value : values) {
            if (aggregatedText.length() > 0) {
                aggregatedText.append(" ");  // Preserve spaces between lines
            }
            aggregatedText.append(value.toString());
        }

        // Emit final cleaned text
        if (aggregatedText.length() > 0) {
            context.write(key, new Text(aggregatedText.toString()));
        }
    }
}
