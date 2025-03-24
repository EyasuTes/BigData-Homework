import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
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

public class HadoopWordFrequency extends Configured implements Tool {

    // Relaxed regex: accepts 6-24 character alphabetic words (case-insensitive)
    private static final Pattern WORD_PATTERN = Pattern.compile("^[a-zA-Z]{6,24}$");
    private static final IntWritable one = new IntWritable(1);

    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            for (String token : tokens) {
                token = token.trim().toLowerCase();  // Normalize to lowercase
                if (isValidWord(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }

        private boolean isValidWord(String token) {
            Matcher matcher = WORD_PATTERN.matcher(token);
            return matcher.matches();
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<String, Integer> wordCounts = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum == 1000) {
                // Task (1): Words with exactly 1000 count
                context.write(new Text("EXACT1000:" + key.toString()), new IntWritable(sum));
            }

            // Store in map for top-100 later
            wordCounts.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Task (3): Top 100 words
            PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(
                    Comparator.comparingInt(Map.Entry::getValue)); // Min-heap

            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                pq.offer(entry);
                if (pq.size() > 100) {
                    pq.poll(); // Keep only top 100 largest
                }
            }

            // Convert to list and reverse to write largest first
            List<Map.Entry<String, Integer>> topList = new ArrayList<>();
            while (!pq.isEmpty()) {
                topList.add(pq.poll());
            }

            for (int i = topList.size() - 1; i >= 0; i--) {
                Map.Entry<String, Integer> entry = topList.get(i);
                context.write(new Text("TOP100:" + entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "HadoopWordFrequency");
        job.setJarByClass(HadoopWordFrequency.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopWordFrequency(), args);
        System.exit(ret);
    }
}


