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

