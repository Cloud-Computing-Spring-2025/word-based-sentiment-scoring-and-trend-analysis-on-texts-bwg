# Project 1: Word-Based Sentiment Scoring and Trend Analysis on Historical Texts

This is Multi-Stage Sentiment Analysis on Historical Literature application is developed with map reduce and hive
Here’s a detailed README and explanation for your Hadoop-based data cleaning project using the 18th and 19th-century literature texts.

---

## **Task1: Data Extraction and Cleaning**

### **Overview:**

This project uses Hadoop MapReduce to clean and process text data from books based on 18th and 19th-century literature. The source content is fetched from [Project Gutenberg](https://www.gutenberg.org/) (or other publicly available sources) and the data is cleaned by removing stop words, punctuation, and irrelevant content.

The main objective is to perform text cleaning such as:

- Removing stop words
- Tokenizing the text
- Removing punctuation and non-alphabetic characters
- Consolidating content for further analysis or use in NLP tasks

### **Input Dataset:**

The input dataset consists of book content from 18th and 19th-century literature available on [Goodreads - List of Top 100 Classic Novels](https://www.goodreads.com/list/show/30). The books’ content is extracted from Project Gutenberg, which offers free access to these works in various formats (such as plain text and HTML). The dataset might include fields like:

- Book ID (unique identifier for the book)
- Year (year of publication)
- Text content (raw book content in the form of long texts)

The data is assumed to be provided as CSV files, where each line represents a book, with the following structure:

1. BookID
2. Title (or Author, depending on the format)
3. Year of publication
4. Content (text of the book)

### **Code Explanation:**

#### **1. `DataCleaningDriver.java`**

This is the main entry point for the MapReduce job. It handles job configuration and input/output paths.

- **Configuration**: Sets up the Hadoop configuration and job.
- **Job Configuration**: Specifies the Mapper (`DataCleaningMapper`) and Reducer (`DataCleaningReducer`) classes. Also sets the output types (key: `Text`, value: `Text`).
- **Input and Output Paths**: The program expects two arguments: input and output paths for reading the raw text files and storing cleaned data, respectively.

```java
public class DataCleaningDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DataCleaningDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Extraction and Cleaning");
        job.setJarByClass(DataCleaningDriver.class);
        job.setMapperClass(DataCleaningMapper.class);
        job.setReducerClass(DataCleaningReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### **2. `DataCleaningMapper.java`**

The Mapper reads each line of the input CSV file, processes it, and outputs cleaned data.

- **Input**: Each line from the CSV file (BookID, Title, Year, Content).
- **Processing**:
  - Skips the header (first line).
  - Extracts the content and removes non-alphabetic characters (i.e., punctuation).
  - Tokenizes the content and removes stopwords (such as “the”, “and”, “is”, etc.).
  - Outputs a key-value pair, where the key is the combination of BookID and Year, and the value is the cleaned content.

```java
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

        // Split CSV line safely
        String[] fields = line.split(",", 4);  // Split only on the first 3 commas

        if (fields.length < 4) {
            return; // Skip malformed lines
        }

        String bookID = fields[0].trim();
        String year = fields[2].trim();
        String content = fields[3].toLowerCase().replaceAll("[^a-z ]", ""); // Remove punctuation

        // Tokenize and remove stopwords
        StringTokenizer tokenizer = new StringTokenizer(content);
        List<String> cleanedWords = new ArrayList<>();

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();
            if (!STOPWORDS.contains(token) && token.length() > 1) {
                cleanedWords.add(token);
            }
        }

        // Emit cleaned text
        if (!cleanedWords.isEmpty()) {
            context.write(new Text(bookID + "," + year), new Text(String.join(" ", cleanedWords)));
        }
    }
}
```

#### **3. `DataCleaningReducer.java`**

The Reducer consolidates the tokenized text for each unique key (BookID, Year) and outputs the final cleaned data.

- **Input**: For each key (BookID, Year), the values are a collection of cleaned words.
- **Processing**: The reducer appends all values (cleaned content) associated with a particular key.
- **Output**: The final cleaned text for each book, identified by the key (BookID, Year).

```java
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
```

### **How to Run:**

1. **Input Files**: The input should be a directory containing CSV files with raw text data (BookID, Year, and Content).
2. **Output Directory**: The output will be a directory containing cleaned text data for each book.
3. **Run the Program**:
   - Compile the code and package it into a JAR file.
   - Use Hadoop's `hadoop jar` command to run the program:
   ```bash
   hadoop jar DataCleaning.jar com.example.DataCleaningDriver <input_path> <output_path>
   ```
4. **View Results**: The cleaned data will be written to the output directory specified.

---

### **Task1 Conclusion:**

This Hadoop MapReduce program processes literature data, cleans it, and prepares it for further text mining or analysis tasks. It helps remove irrelevant data, tokenize the content, and eliminate stop words to make the content more suitable for applications like NLP and sentiment analysis.

### Task 2: Word Frequency Analysis with Lemmatization

Overview
This task builds upon Task 1 (Data Cleaning) by performing word frequency analysis while applying lemmatization to normalize word forms. The goal is to compute the frequency of each word’s base form (lemma) across different books and years.

Using Hadoop MapReduce, this program processes cleaned text from Task 1, splits it into individual words, applies lemmatization using an NLP library (such as WordNet Lemmatizer), and calculates word frequency across different historical texts.

Objectives
Process cleaned text data from Task 1.

Split sentences into words (tokenization).

Apply lemmatization to normalize words to their base form (e.g., "running" → "run").

Count the frequency of each lemma per book and year using MapReduce.

Produce a structured dataset listing word frequencies per book and year.

Input Dataset
The input dataset consists of cleaned book content from Task 1. The data is formatted as:

BookID Year Cleaned Text
101 1818 "monster create fear creature terrifying"
102 1847 "love strong man woman desire"
BookID: Unique identifier for each book.

Year: Year of publication.

Cleaned Text: The processed text, free from punctuation and stop words.

This dataset is stored in CSV format, where each line represents a book's processed content.

### Implementation Details

1. WordFrequencyDriver.java (Main Entry Point)
   The driver class is responsible for:

Configuring and setting up the Hadoop Job.

Specifying the Mapper (WordFrequencyMapper.java) and Reducer (WordFrequencyReducer.java).

Defining input and output file paths.

### Code:

java
package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordFrequencyDriver {
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
if (otherArgs.length != 2) {
System.err.println("Usage: WordFrequencyDriver <input path> <output path>");
System.exit(2);
}

        Job job = Job.getInstance(conf, "Word Frequency Analysis with Lemmatization");
        job.setJarByClass(WordFrequencyDriver.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setReducerClass(WordFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

} 2. WordFrequencyMapper.java (Mapping Phase)
The Mapper processes each line of the input file, tokenizes words, applies lemmatization, and emits key-value pairs:

Key → (BookID, Lemma, Year)
Value → 1 (to indicate word occurrence)

Key Features:
Tokenization: Splits text into individual words.

Lemmatization: Converts words to their base forms.

Output Format: Emits (bookID, lemma, year) -> 1 pairs.

### Code:

java
package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
import net.sf.extjwnl.dictionary.Dictionary;
import net.sf.extjwnl.data.IndexWord;
import net.sf.extjwnl.data.POS;

public class WordFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
private static final IntWritable one = new IntWritable(1);
private Dictionary dictionary;

    @Override
    protected void setup(Context context) throws IOException {
        try {
            dictionary = Dictionary.getDefaultResourceInstance();
        } catch (Exception e) {
            throw new IOException("Error initializing WordNet dictionary", e);
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] fields = line.split(",", 3); // Split only into BookID, Year, Content

        if (fields.length < 3) {
            return; // Skip malformed lines
        }

        String bookID = fields[0].trim();
        String year = fields[1].trim();
        String content = fields[2].trim();

        StringTokenizer tokenizer = new StringTokenizer(content);

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase();

            // Apply lemmatization
            String lemma = getLemma(word);

            // Emit (bookID, lemma, year) -> 1
            context.write(new Text(bookID + "," + lemma + "," + year), one);
        }
    }

    private String getLemma(String word) {
        try {
            IndexWord indexWord = dictionary.lookupIndexWord(POS.NOUN, word);
            if (indexWord != null) {
                return indexWord.getLemma();
            }
        } catch (Exception e) {
            return word; // Default to original word if lemmatization fails
        }
        return word;
    }

} 3. WordFrequencyReducer.java (Reducing Phase)
The Reducer aggregates word counts per (BookID, Lemma, Year).

Input: (bookID, lemma, year) -> [1, 1, 1, ...]
Processing: Sum occurrences of each lemma.
Output: (bookID, lemma, year) -> frequency

### Code:

java
package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
@Override
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable value : values) {
sum += value.get(); // Sum word occurrences
}
context.write(key, new IntWritable(sum));
}
}
How to Run the Program

### Step 1: Prepare Input Files

Ensure the input dataset (cleaned text) is stored in HDFS:

bash
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -put cleaned_books.csv /user/hadoop/input/

### Step 2: Compile and Package

Compile the Java program and create a JAR file:

bash
hadoop com.sun.tools.javac.Main WordFrequency*.java
jar cf WordFrequency.jar WordFrequency*.class

### Step 3: Run the Hadoop Job

Execute the MapReduce job:

bash
hadoop jar WordFrequency.jar com.example.WordFrequencyDriver /user/hadoop/input /user/hadoop/output

### Step 4: View the Results

Fetch the results from HDFS:

bash
hdfs dfs -cat /user/hadoop/output/part-r-00000

### Task 2 Conclusion

This Hadoop MapReduce job analyzes word frequency while applying lemmatization to standardize word forms. The output is a structured dataset showing the occurrence of each lemma per book and year, making it useful for trend analysis, topic modeling, and sentiment analysis in historical literature.

This task sets the foundation for further NLP analysis, including sentiment scoring, thematic trends, and historical text comparison. 🚀

Here’s your README file in the proper format:

---

# Task 3: Sentiment Scoring

## Overview

This task assigns sentiment scores to texts by mapping words (or their lemmatized forms) to sentiment values using a sentiment lexicon. The sentiment scores are then aggregated and associated with individual books and their respective years.

## Prerequisites

Before running this task, ensure you have the following:

- **Hadoop and MapReduce** set up in Docker.
- **Sentiment lexicon file (`afinn.txt`)** available in HDFS.
- **Input dataset from Task 2** available in HDFS.
- **Java & Maven installed** in the environment.

## Setup Instructions

### 1. Navigate to the Task Directory

Open a terminal and move to the directory containing Task 3 files:

```bash
cd task3
```

### 2. Start the Hadoop Cluster

Ensure that the Hadoop cluster is running in Docker:

```bash
docker compose up -d
```

### 3. Build the Project

Compile the Java project using Maven:

```bash
mvn clean package
```

### 4. Copy JAR File to Hadoop Container

Move the compiled JAR file to the Hadoop ResourceManager container:

```bash
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 5. Copy Dataset to Hadoop Container

Transfer the processed dataset from Task 2 into the Hadoop container:

```bash
docker cp task2 resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 6. Access the Hadoop Container

Connect to the running Hadoop container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory inside the container:

```bash
cd /opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 7. Set Up HDFS for Input Data

Create an HDFS directory for input data:

```bash
hadoop fs -mkdir -p /input/dataset
```

Move the dataset into HDFS:

```bash
hadoop fs -put task2 /input/dataset
```

### 8. Run the Sentiment Analysis MapReduce Job

Execute the MapReduce job for sentiment scoring:

```bash
hadoop jar /opt/hadoop-2.7.4/share/hadoop/mapreduce/DocumentSimilarity-0.0.1-SNAPSHOT.jar com.example.controller.SentimentAnalysisDriver /input/dataset/task2/output2/part-r-00000 /output1
```

### 9. Verify Output

Check the results by retrieving the output files:

```bash
hadoop fs -cat /output1/*
```

### 10. Copy Output Data from HDFS to Local Machine

#### Step 1: Copy Output from HDFS to Hadoop Container

```bash
hdfs dfs -get /output1 /opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

#### Step 2: Exit the Container

```bash
exit
```

#### Step 3: Copy Output from the Container to Local System

```bash
docker cp resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/output1/ ./output
```

## Conclusion

This completes the sentiment scoring process using Hadoop MapReduce. The output files contain sentiment scores for texts, aggregated by book and year.

---



# Task 4: Trend Analysis & Aggregation

## Overview

This task aggregates sentiment scores and word frequencies over broader time intervals (e.g., by decade) to identify long-term trends and potential correlations with historical events. The data from previous tasks will be processed using a Hadoop MapReduce job to group by decade and analyze trends.

## Prerequisites

Before running this task, ensure you have the following:

- **Hadoop and MapReduce** set up in Docker.
- **Sentiment scoring output (`output1/` from Task 3)** available in HDFS.
- **Java & Maven installed** in the environment.

## Setup Instructions

### 1. Navigate to the Task Directory

Open a terminal and move to the directory containing Task 4 files:

```bash
cd task4
```

### 2. Start the Hadoop Cluster

Ensure that the Hadoop cluster is running in Docker:

```bash
docker compose up -d
```

### 3. Build the Project

Compile the Java project using Maven:

```bash
mvn clean package
```

### 4. Copy JAR File to Hadoop Container

Move the compiled JAR file to the Hadoop ResourceManager container:

```bash
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 5. Ensure Input Data is Available in HDFS

Check if the sentiment scoring output (`output1/`) is already in HDFS:

```bash
hadoop fs -ls /output1
```

If not, copy the dataset into HDFS:

```bash
hadoop fs -put output1 /input/dataset
```

### 6. Run the Trend Analysis MapReduce Job

Execute the MapReduce job for trend analysis and aggregation:

```bash
hadoop jar /opt/hadoop-2.7.4/share/hadoop/mapreduce/DocumentSimilarity-0.0.1-SNAPSHOT.jar com.example.controller.TrendAnalysisDriver /output1 /output2
```

### 7. Verify Output

Check the results by retrieving the output files:

```bash
hadoop fs -cat /output2/*
```

### 8. Copy Output Data from HDFS to Local Machine

#### Step 1: Copy Output from HDFS to Hadoop Container

```bash
hdfs dfs -get /output2 /opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

#### Step 2: Exit the Container

```bash
exit
```

#### Step 3: Copy Output from the Container to Local System

```bash
docker cp resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/output2/ ./output
```


## Conclusion

This completes the trend analysis and aggregation process. The output files summarize sentiment scores and word frequencies over decades, providing insights into historical trends. Visualization can further enhance the interpretation of these results.

-----
# Task 5: Bigram Analysis using Hive UDF

## Overview

This task involves extracting and analyzing bigrams (pairs of consecutive words) from the lemmatized text output of Task 2. A custom Hive User-Defined Function (UDF) is implemented in Java to process text data and generate bigrams. The results are stored in a Hive table for further analysis.

## Prerequisites

Before running this task, ensure you have the following:

- **Hadoop and Hive** set up in Docker.
- **Lemmatized text dataset (`output2/` from Task 2)** available in HDFS.
- **Java & Maven installed** in the environment.

## Setup Instructions

### 1. Navigate to the Task Directory

Open a terminal and move to the directory containing Task 5 files:

```bash
cd task5
```

### 2. Start the Hadoop and Hive Cluster

Ensure that the Hadoop cluster and Hive metastore are running in Docker:

```bash
docker compose up -d
```

### 3. Build the UDF

Compile the Java UDF using Maven:

```bash
mvn clean package
```

### 4. Copy JAR File to Hadoop Container

Move the compiled JAR file to the Hive container:

```bash
docker cp target/BigramAnalysis-0.0.1-SNAPSHOT.jar hive-server:/opt/hive/lib/
```

### 5. Restart Hive to Load the UDF

Access the Hive container and restart the service:

```bash
docker exec -it hive-server /bin/bash
hive --service metastore &
hive --service hiveserver2 &
exit
```

### 6. Ensure Input Data is Available in HDFS

Check if the lemmatized text data (`output2/`) is already in HDFS:

```bash
hadoop fs -ls /output2
```

If not, copy the dataset into HDFS:

```bash
hadoop fs -put output2 /input/dataset
```

### 7. Create a Hive Table for Lemmatized Text

Open Hive and create a table for storing lemmatized text:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS lemmatized_text (
    bookID STRING,
    year INT,
    text STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/input/dataset/output2';
```

### 8. Register the UDF in Hive

```sql
CREATE TEMPORARY FUNCTION extract_bigrams AS 'task5.BigramUDF';
```

### 9. Run Bigram Analysis

Extract bigrams from the text data:

```sql
SELECT bookID, year, extract_bigrams(text) AS bigrams 
FROM lemmatized_text
LIMIT 10;
```

### 10. Aggregate Bigram Frequencies

Count occurrences of each bigram:

```sql
SELECT bigram, COUNT(*) AS frequency 
FROM ( 
    SELECT explode(extract_bigrams(text)) AS bigram 
    FROM lemmatized_text 
) bigram_table
GROUP BY bigram
ORDER BY frequency DESC
LIMIT 20;
```

### 11. Save Results into a Hive Table

Create a table to store bigram frequencies:

```sql
CREATE TABLE bigram_frequencies AS 
SELECT bigram, COUNT(*) AS frequency 
FROM ( 
    SELECT explode(extract_bigrams(text)) AS bigram 
    FROM lemmatized_text 
) bigram_table
GROUP BY bigram;
```

### 12. Query for Sentiment-Related Bigrams

Analyze sentiment-related bigrams based on prior sentiment scoring:

```sql
SELECT bf.bigram, bf.frequency, s.sentiment_score
FROM bigram_frequencies bf
JOIN sentiment_scores s ON bf.bigram = s.word
ORDER BY bf.frequency DESC;
```

### 13. Retrieve Output Data

Retrieve the bigram frequency results from Hive:

```sql
SELECT * FROM bigram_frequencies LIMIT 50;
```

Export the results to a local file:

```sql
INSERT OVERWRITE LOCAL DIRECTORY '/output/bigram_results' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * FROM bigram_frequencies;
```

Copy the results from the Hive container to the local machine:

```bash
docker cp hive-server:/output/bigram_results ./bigram_results
```

## Conclusion

This completes the bigram analysis using Hive UDF. The results provide insights into common word pairings and their frequency in historical texts, enabling further exploration of linguistic patterns and sentiment trends.

