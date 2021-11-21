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

public class WordCount {

    /**
     * TODO: Test on e-books
     * TODO: Remove header from e-books
     */

    /**
     * n-gram size
     */
    private static int n = 3;

    public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Removes whitespace and punctuations from a given string
            // Then separates string into individual words/tokens
            StringTokenizer st = new StringTokenizer(value.toString().replaceAll("\\p{Punct}", "").toLowerCase());

            Text nGram = new Text();

            // A queue is used as a buffer for n-grams
            Queue<String> queue = (n == 1 ? null : new LinkedList<String>());   // Declare only when needed

            while (st.hasMoreTokens()) {
                String currentToken = st.nextToken();

                if (n == 1) {
                    // Skips expensive routine for n-grams, use current token instead
                    nGram.set(currentToken);
                    context.write(nGram, new IntWritable(1));

                } else if (queue.size() == n-1) {   // n-1 saves one loop per n-gram.
                    // Does multiple things here:
                    // The head of the queue is removed,
                    // peekList() then appends the rest of the queue
                    // as queue size is n-1, the current token is appended.
                    nGram.set(queue.poll() + " " + peekList(queue) + currentToken);
                    context.write(nGram, new IntWritable(1));
                }

                if (n > 1) queue.add(currentToken);     // only add if queue exists
            }
        }

        /**
         * Reads all strings in a queue and concatinates into a space-delimited string
         * @param queue A queue of Strings
         * @return Space-delimited String of all elements
         */
        private static String peekList(Queue<String> queue) {
            StringBuilder sb = new StringBuilder();
            queue.forEach(x -> sb.append(x).append(" "));

            return sb.toString();
        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            IntWritable result = new IntWritable();

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
            System.out.println("WARNING: n-gram length not provided, n=3 is assumed.");
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
