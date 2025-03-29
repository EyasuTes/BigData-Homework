import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopWordPairAnalysis extends Configured implements Tool {

    private static final Pattern WORD_PATTERN = Pattern.compile("^[a-zA-Z]{6,24}$");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9]+(\\.[0-9]+)?$");
    private static final IntWritable one = new IntWritable(1);

    public static class PairMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text pair = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            List<String> cleanTokens = new ArrayList<>();

            for (String token : tokens) {
                token = token.trim().toLowerCase();
                if (isValidWord(token) || isValidNumber(token)) {
                    cleanTokens.add(token);
                }
            }

            for (int i = 0; i < cleanTokens.size(); i++) {
                String center = cleanTokens.get(i);

                for (int offset = -2; offset <= 2; offset++) {
                    if (offset == 0) continue;
                    int j = i + offset;
                    if (j >= 0 && j < cleanTokens.size()) {
                        String neighbor = cleanTokens.get(j);

                        // (2) word-word pairs
                        if (isValidWord(center) && isValidWord(neighbor)) {
                            pair.set("WW:" + center + ":" + neighbor);
                            context.write(pair, one);
                        }

                        // (4) number-word pairs
                        if (isValidNumber(center) && isValidWord(neighbor)) {
                            pair.set("NW:" + center + ":" + neighbor);
                            context.write(pair, one);
                        }
                    }
                }
            }
        }

        private boolean isValidWord(String token) {
            return WORD_PATTERN.matcher(token).matches();
        }

        private boolean isValidNumber(String token) {
            if (!NUMBER_PATTERN.matcher(token).matches()) return false;
            return token.length() >= 4 && token.length() <= 16;
        }
    }

    public static class PairReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> numberWordPairs = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String[] parts = key.toString().split(":", 3);
            String prefix = parts[0];
            String actualPair = parts[1] + ":" + parts[2];

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if ("WW".equals(prefix)) {
                if (sum == 1000) {
                    context.write(new Text("EXACT1000_PAIR:" + actualPair), new IntWritable(sum));
                }
            } else if ("NW".equals(prefix)) {
                numberWordPairs.put(actualPair, sum);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output top-100 number-word pairs
            PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(
                    Comparator.comparingInt(Map.Entry::getValue));

            for (Map.Entry<String, Integer> entry : numberWordPairs.entrySet()) {
                pq.offer(entry);
                if (pq.size() > 100) pq.poll();
            }

            List<Map.Entry<String, Integer>> top = new ArrayList<>();
            while (!pq.isEmpty()) top.add(pq.poll());

            for (int i = top.size() - 1; i >= 0; i--) {
                Map.Entry<String, Integer> entry = top.get(i);
                context.write(new Text("TOP100_NUMWORD:" + entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "HadoopWordPairAnalysis");
        job.setJarByClass(HadoopWordPairAnalysis.class);

        job.setMapperClass(PairMapper.class);
        job.setReducerClass(PairReducer.class);
        //job.setCombinerClass(PairReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileInputFormat.setInputDirRecursive(job, true); // Very important
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HadoopWordPairAnalysis(), args);
        System.exit(exitCode);
    }
}
