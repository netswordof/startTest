package wordcountexample;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by netswordof on 2018/11/18.
 */
public class WordCountDriver  {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(WordCountDriver.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(CombineTextInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //FileInputFormat.setInputPaths(job,new Path(PropertiesUtil.getProperties("wordcountIn")));
        //FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop107:9000/hadoop_data/wordcountIn"));
        FileInputFormat.setInputPaths(job,new Path("har://hdfs-hadoop107:9000/hadoop_data/wordcountArchive/input.har/input_2"));
        //FileOutputFormat.setOutputPath(job,new Path(PropertiesUtil.getProperties("wordcountOut")));
        //FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop107:9000/hadoop_data/wordcountOut"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop107:9000/hadoop_data/wordcountOut"));
        boolean c = job.waitForCompletion(true);
        System.out.println(c);
    }
}
