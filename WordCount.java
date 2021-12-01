import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCount {

    /**
     * n-gram size
     */
    private static int n = 3;

    /**
     * Custom partitioner class. This assigns n-grams to partitions according to their startig character.
     * Up to 26 reducers/partitions can be used, minimum one for each character in the alphabet.
     */
    public static class WCPartitioner extends Partitioner<Text, IntWritable> {
        private static int range = ('z' + 1) - 'a';

        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (numPartitions > 26)
                throw new IllegalArgumentException("Only up to 26 reducers are supported, since there's 26 letters in the alphabet.");

            int current = key.toString().charAt(0);                 // offset character so 'a' starts at 0

            // assign n-grams that starts with a number to be on the first partition
            // lazy workaround but e-books don't have that many numbers compared to letters
            if (current < 'a')
                return 0;

            // cutoff point for each partition
            int boundary = range / numPartitions;

            // by truncating the result, this (should've) give a partition number in the range of no. of reducers
            int partitionNumber = (current - 'a') / boundary;

            // float -> int doesn't truncate properly so partition number is clamped for later chars
            // may have a performance impact on jobs with lower number of reducers (letters 't' and 'y' appears often in English)
            // performance loss becomes less relevant as the number of reducers approaches 26
            return (partitionNumber >= numPartitions ? numPartitions - 1 : partitionNumber);
        }
    }

    public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static IntWritable one = new IntWritable(1);
        private static Text nGram = new Text();

        // A queue is used as a buffer for n-grams
        // Declare only when needed
        // Required to be global to create n-grams across newlines
        private static Queue<String> queue = (n == 1 ? null : new LinkedList<String>());

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Removes whitespace and punctuations from a given string
            // Then separates string into individual words/tokens
            StringTokenizer st = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z0-9\\s]+", "").toLowerCase());

            while (st.hasMoreTokens()) {
                String currentToken = st.nextToken();

                if (n == 1) {
                    // Skips expensive routine for n-grams, use current token instead
                    nGram.set(currentToken);
                    context.write(nGram, one);

                } else if (queue.size() == n-1) {   // n-1 saves one loop per n-gram.
                    // Does multiple things here:
                    // The head of the queue is removed,
                    // peekList() then appends the rest of the queue
                    // as queue size is n-1, the current token is appended.
                    nGram.set(peekList() + currentToken);
                    context.write(nGram, one);
                }

                if (n > 1) queue.add(currentToken);     // only add if queue exists
            }
        }

        /**
         * Reads all strings in a queue and concatinates into a space-delimited string
         * @param queue A queue of Strings
         * @return Space-delimited String of all elements
         */
        private static String peekList() {
            // Memory optimisation for large n-grams
            StringBuilder sb = new StringBuilder(queue.poll()).append(" ");
            queue.forEach(word -> sb.append(word).append(" "));

            return sb.toString();
        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            n = Integer.parseInt(args[2]);
            System.out.println("INFO: n-gram length is n=" + n);
        } catch (Exception e) {
            // (B921412 % 5 + 1) = 3; therefore, trigrams are default.
            System.out.println("WARNING: n-gram length not provided, n=3 is assumed.");
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count");

        // Disk I/O optimisation by using LZO compression
        // Introduces minor CPU overhead
        conf.set("mapreduce.map.output.compress", "true");
        // job.setNumReduceTasks(7);

        job.setJarByClass(WordCount.class);
        job.setCombinerClass(WCReducer.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(WCPartitioner.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
