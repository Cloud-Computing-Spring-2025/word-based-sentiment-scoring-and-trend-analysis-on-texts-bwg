package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.SentimentMapper;
import com.example.SentimentReducer;

public class SentimentAnalysisDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SentimentAnalysisDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create a new Hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentiment Analysis");

        // Set the Jar file
        job.setJarByClass(SentimentAnalysisDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output paths from arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


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

This README provides a structured guide to execute Task 3. Let me know if you need any refinements! 🚀