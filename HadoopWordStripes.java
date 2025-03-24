import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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

public class HadoopWordStripes extends Configured implements Tool {

    private final static IntWritable one = new IntWritable(1);

    // Regular expression for words (lowercase letters and dashes, 6-24 characters)
    private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z-]{6,24}$");

    // Regular expression for numbers (digits, decimal points, at most one leading dash, 4-16 characters)
    private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9]+(\\.[0-9]+)?$");

    public static class Stripe extends MapWritable {

        public void add(String w) {
            IntWritable count = new IntWritable(0);
            if (containsKey(new Text(w))) {
                count = (IntWritable) get(new Text(w));
                remove(new Text(w));
            }
            count = new IntWritable(count.get() + one.get());
            put(new Text(w), count);
        }

        public void merge(Stripe from) {
            for (Writable fromKey : from.keySet())
                if (containsKey(fromKey))
                    put(fromKey, new IntWritable(((IntWritable) get(fromKey)).get() + ((IntWritable) from.get(fromKey)).get()));
                else
                    put(fromKey, (IntWritable) from.get(fromKey));
        }

        @Override
        public String toString() {
            StringBuilder buffer = new StringBuilder();
            for (Writable key : keySet())
                buffer.append(" ").append(key).append(":").append(get(key));
            return buffer.toString();
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Stripe> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split("\\s+"); // Split by spaces

            for (int i = 0; i < tokens.length; i++) {
                String currentWord = tokens[i].trim();

                // Check if the current word is valid
                if (!isValidWord(currentWord) && !isValidNumber(currentWord)) {
                    continue; // Skip invalid tokens
                }

                Stripe stripe = new Stripe();

                // Look at the previous word
                if (i > 0) {
                    String prevWord = tokens[i - 1].trim();
                    if (isValidWord(prevWord) || isValidNumber(prevWord)) {
                        stripe.add(prevWord);
                    }
                }

                // Look at the next word
                if (i < tokens.length - 1) {
                    String nextWord = tokens[i + 1].trim();
                    if (isValidWord(nextWord) || isValidNumber(nextWord)) {
                        stripe.add(nextWord);
                    }
                }

                context.write(new Text(currentWord), stripe);
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

    public static class Reduce extends Reducer<Text, Stripe, Text, Stripe> {

        @Override
        public void reduce(Text key, Iterable<Stripe> values, Context context)
                throws IOException, InterruptedException {

            Stripe globalStripe = new Stripe();

            for (Stripe localStripe : values)
                globalStripe.merge(localStripe);

            context.write(key, globalStripe);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "HadoopWordStripes");
        job.setJarByClass(HadoopWordStripes.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Stripe.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopWordStripes(), args);
        System.exit(ret);
    }
}
