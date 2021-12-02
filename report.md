# Cloud Computing Report

- [Cloud Computing Report](#cloud-computing-report)
  - [Build and Usage Instructions](#build-and-usage-instructions)
    - [Building MapReduce](#building-mapreduce)
    - [Running MapReduce](#running-mapreduce)
  - [Implementation](#implementation)
    - [Minimal Features](#minimal-features)
    - [Additional Features](#additional-features)
      - [Any n-gram mapper](#any-n-gram-mapper)
      - [Global Sorting](#global-sorting)
    - [MapReduce Optimisations](#mapreduce-optimisations)
      - [Caching Objects & Writables](#caching-objects--writables)
      - [Using StringBuilder](#using-stringbuilder)
      - [Using a Combiner](#using-a-combiner)
      - [Enabling LZO Compression](#enabling-lzo-compression)

## Build and Usage Instructions

### Building MapReduce

```bash
hadoop com.sun.tools.javac.Main WordCount.java
jar -cf wc.jar WordCount*.class
```

### Running MapReduce

```bash
hadoop jar wc.jar WordCount input-path output-path [n-gram number]
# n-gram number is the length of the n-gram.
# This defaults to 3 (trigrams) if left empty
```

## Implementation

### Minimal Features

A modified implementation of mapper class `WCMapper` is used. While the reducer class `WCReducer` remains unchanged.

When the method `map()` is run, a `StringTokenizer` is used to split each line into a list of words. I've also pre-processed the text input further by removing any punctuation and convert the text to all lower case letters. Pre-processing at this stage saves time and expensive I/O usage from post-processing at the reducer stage.

The mapper now uses two extra variables to store the previous tokens. Along with the current token, these are then combined into a trigram as $(\text{B}921412 \ \text{`mod`} \ 5) + 1 = 3$.

```java
/**
 * This code now exists in documentation only.
 */
public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(
          value.toString().replaceAll("[^a-zA-Z0-9\\s]+", "").toLowerCase());

        IntWritable one = new IntWritable(1);
        Text nGram = new Text();

        String token1 = null;
        String token2 = null;

        while (st.hasMoreTokens()) {
            String currentToken = st.nextToken();

            if (token1 != null && token2 != null) {
                nGram.set(token1 + " " + toeken2 + " " + currentToken);
                context.write(nGram, one);
            }

            token1 = token2;
            token2 = currentToken;
        }
    }
}
```

Once compiled, the MapReduce program produces the following output:

```md
# Input
How can a clam cram in a clean cream can?

# Output
a clam cram	1
a clean cream	1
can a clam	1
clam cram in	1
clean cream can	1
cram in a	1
how can a	1
in a clean	1
```

### Additional Features

#### Any n-gram mapper

To change the n-gram size, enter a number in the third argument as shown in the [Running MapReduce](#running-mapreduce) section. If left empty, it defaults to `n = 3`.

To accommodate any n-gram length, I've decided to use a doubly linked list to store each token instead of a fixed length array. By using a queue data type, I can easily manipulate it by using `.poll()` to select and remove the head element, `.add()` to append a token, and iterate through the rest of the queue using `.forEach()`.

While an array may be faster and less resource intensive than a queue, that only applies at lower ranges of n-gram lengths. At larger n-gram sizes, it may take more CPU time to iterate and shift all elements forward to free space for the next token.

```java
private static Queue<String> queue = (n == 1 ? null : new LinkedList<String>());
```

The mapper then iterates through all tokens, only writing a key-pair once the queue has reached the specified n-gram size. If the n-gram size is `n = 1`, it skips the more expensive queue entirely.

```java
while (st.hasMoreTokens()) {
    String currentToken = st.nextToken();

    if (n == 1) {
        // Skips expensive routine for n-grams, use current token instead
        nGram.set(currentToken);
        context.write(nGram, one);

    } else if (queue.size() == n - 1) { // n-1 saves one loop per n-gram.
        nGram.set(peekList() + currentToken);
        context.write(nGram, one);
    }

    if (n > 1)
        queue.add(currentToken); // only add if queue exists
}
```

The method `peekList()` iterates through the list, removing the head element, and concatenates the tokens into one n-gram. I've also taken advantage of lambda expressions introduced in Java 8 to increase readability.

```java
private static String peekList() {
    // Memory optimisation for large n-grams
    StringBuilder sb = new StringBuilder(queue.poll()).append(" ");
    queue.forEach(word -> sb.append(word).append(" "));

    return sb.toString();
}
```

#### Global Sorting

As Hadoop MapReduce uses the `HashPartitioner` by default to assign work to reducers, there is very little control on which reducer gets which key-value pair. Therefore, I've decided to create a custom partitioner.

This is a deterministic partitioner which groups n-grams based on their 1<sup>st</sup> character. In consequence, the number of partitions and/or reducers are limited to 26; one for each letter.

As characters in Java can be treated as integers, I came up with a formula that determines which letter is assigned to a particular partition. Aside from the hard limit of 26, this can scale with any number of reducers specified within that range.

This formula is used to calculate the partition number (partition no. starts at $0$):
$$
\frac{\text{Current} - \text{Offset}}{\text{Character Range} \div \text{No. of Reducers}}
$$

Where $\text{Offset}$ is the starting character ($a$ in this case) and $\text{Character Range}$ is the length of the alphabet.

For example, to calculate the partition no. of 'g' with seven reducers:
$$
\frac{g - a}{26 \div 7} = \frac{103 - 97}{26 \div 7} = 1.615 \text{ (4 s.f.)} = 1 \text{ (trunc.)}
$$

However, typecasting integers onto a float may introduce inaccuracies. Therefore, I've clamped the partition number to be $\text{Number of Reducers} - 1$.

In the case of when an n-gram starts with a numerical character, it is automatically assigned to partition $0$. My justification is that n-grams starting with a number would appear less often than those with alphabetical characters, so the performance impact isn't very noticeable.

```java
public static class WCPartitioner extends Partitioner<Text, IntWritable> {
    private static int range = ('z' + 1) - 'a';

    public int getPartition(Text key, IntWritable value, int numPartitions) {
        if (numPartitions > 26)
            throw new IllegalArgumentException(
                    "Only up to 26 reducers are supported, since there's 26 letters in the alphabet.");

        int current = key.toString().charAt(0);

        // assign n-grams that starts with a number to be on the first partition
        if (current < 'a')
            return 0;

        // cut-off point for each partition
        int boundary = range / numPartitions;

        // by truncating the result, this (should've) give a partition number in the
        // range of no. of reducers
        int partitionNumber = (current - 'a') / boundary;

        // float -> int conversion inaccuracy workaround
        return (partitionNumber >= numPartitions ? numPartitions - 1 : partitionNumber);
    }
}
```

### MapReduce Optimisations

#### Caching Objects & Writables

Initialising a new object every time is often a costly operation. Therefore, I've declared reusable objects globally for its class.

```java
public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static IntWritable one = new IntWritable(1);
    private static Text nGram = new Text();
```

#### Using StringBuilder

When programmatically concatenating strings in a loop, I've decided to use a StringBuilder instead of concatenating strings normally. This is because Strings are immutable. This means when you concatenate, an entirely new String object is created and the old one is discard in the garbage collector.

With StringBuilders, `.append()` modifies the original objects, so there's no extra garbage left over.

This optimisation is greatly realised with increasingly larger n-grams.

```java
StringBuilder sb = new StringBuilder(queue.poll()).append(" ");
queue.forEach(word -> sb.append(word).append(" "));
```

#### Using a Combiner

I've set the job's config to use a combiner to reduce the time taken for data transfer between mapper and reducer. This results in decreased amount of data that needed to be processed by the reducer.

```java
job.setCombinerClass(WCReducer.class);
```

#### Enabling LZO Compression

During a MapReduce task, disk I/O usage often peaks at the shuffling stage. This means the CPU often waits around for data to be fetched from its storage bucket and decreases throughput. Although LZO adds a little bit of CPU overhead, it saves time by reducing the amount of disk IO during the shuffle stage as less data are read from slow hard disks.

```java
conf.set("mapreduce.map.output.compress", "true");
```
