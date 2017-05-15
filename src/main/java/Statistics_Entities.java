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

public class Statistics_Entities {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{

        private final static LongWritable one = new LongWritable(1);
        public static final HashSet<String> stopwordsLoader = new StopwordsLoader().getDictionary();

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
            final String items = row.toString();

            String[] stringArray = items.split("\t");

            if (!stringArray[7].equals("null;")) {
                for (String word : stringArray[7].split(";")) {

                    String entityName = word.split(":")[1];
                    if(!stopwordsLoader.contains(entityName) &&
                            entityName.length() != 1 &&
                            !entityName.contains("_film") &&
                            !entityName.contains("_song") &&
                            !entityName.contains("_series") &&
                            !entityName.contains("_album") &&
                            !entityName.contains("%28film%29") &&
                            !entityName.contains("%28song%29") &&
                            !entityName.contains("%28tv_series%29") &&
                            !entityName.contains("album%29")){
                        context.write(new Text(entityName), one);
                    }
                }


            }else{
                context.write(new Text("NULL_ENTITIES"), one);

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
        Job job = Job.getInstance(conf, Statistics_Entities.class.getName());

        job.setJarByClass(Statistics_Entities.class);
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