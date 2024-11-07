# Steps to convert MapReduce to Spark

- Identify the map and reduce tasks in MapReduce
- Load data with Spark to RDD
- Transform Data:
  - Map Phase: Use transformations like map(), flatMap(), or filter() to replicate the behavior of the Map phase in MapReduce.
  - Shuffle Phase: If your MapReduce job involves shuffling (like grouping), you can use groupByKey() or reduceByKey() in Spark.
  - Reduce Phase: Use transformations like reduce(), aggregate(), or combineByKey() to perform the Reduce operation.

# Example: Word Count

## MapReduce version
```python
# Mapper
def mapper(line):
    for word in line.split():
        emit(word, 1)


# Reducer
def reducer(word, counts):
    total = sum(counts)
    emit(word, total)


# Driver code
input_data = read_input("input.txt")
mapped_data = map(mapper, input_data)
reduced_data = reduce(reducer, mapped_data)
write_output("output.txt", reduced_data)
```

## Spark version
```python
from pyspark import SparkContext

# Initialize Spark context
sc = SparkContext("local", "WordCount")

# Load data
input_data = sc.textFile("input.txt")

# Transform data: Map phase
# Split lines into words and create a pair RDD (word, 1)
word_counts = input_data.flatMap(lambda line: line.split()).map(lambda word: (word, 1))

# Shuffle phase and Reduce phase
# Reduce by key (word) to count occurrences
reduced_word_counts = word_counts.reduceByKey(lambda a, b: a + b)

# Collect results and write to output
reduced_word_counts.saveAsTextFile("output.txt")

# Stop the Spark context
sc.stop()
```
