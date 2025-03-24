import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopWordPairs extends Configured implements Tool {

    private final static IntWritable one = new IntWritable(1);

    // Regular expression for words (lowercase letters and dashes, 6-24 characters)
    private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z-]{6,24}$");

    // Regular expression for numbers (digits, decimal points, at most one leading dash, 4-16 characters)
    private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9]+(\\.[0-9]+)?$");

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text pair = new Text();
        private String lastWord = null; // Store previous word

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split("\\s+"); // Split by spaces

            for (String currentWord : tokens) {
                currentWord = currentWord.trim(); // Remove whitespace

                // Check if the current word is valid
                if (!isValidWord(currentWord) && !isValidNumber(currentWord)) {
                    continue; // Skip invalid tokens
                }

                // If there is a previous valid word, emit the word pair
                if (lastWord != null) {
                    pair.set(lastWord + ":" + currentWord);
                    context.write(pair, one);
                }

                // Update lastWord to currentWord for the next iteration
                lastWord = currentWord;
            }
        }

        // Check if token is a valid word
        private boolean isValidWord(String token) {
            Matcher matcher = WORD_PATTERN.matcher(token);
            return matcher.matches();
        }

        // Check if token is a valid number
        private boolean isValidNumber(String token) {
            Matcher matcher = NUMBER_PATTERN.matcher(token);
            if (matcher.matches()) {
                return token.length() >= 4 && token.length() <= 16; // Ensure length constraint
            }
            return false;
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values)
                sum += value.get();

            context.write(key, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "HadoopWordPairs");
        job.setJarByClass(HadoopWordPairs.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs(), args);
        System.exit(ret);
    }
}
