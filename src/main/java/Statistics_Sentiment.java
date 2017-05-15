/**
 * Created by iosifidis on 08.08.16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;

public class Statistics_Sentiment {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{

        private final static LongWritable one = new LongWritable(1);
        public static final HashSet<String> stopwordsLoader = new StopwordsLoader().getDictionary();

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
            final String items = row.toString();

            String[] stringArray = items.split("\t");
            String sentString = stringArray[8];
            int pos = Integer.valueOf(sentString.split(" ")[0]);
            int neg = Integer.valueOf(sentString.split(" ")[1]);
            if (neg == -1 && pos == 1) {
                context.write(new Text("neutral_sentiment"), one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Statistics_Sentiment.class.getName());

        job.setJarByClass(Statistics_Sentiment.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}