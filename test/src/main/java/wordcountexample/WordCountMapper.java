package wordcountexample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by netswordof on 2018/11/18.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        Text k = new Text();
        IntWritable v = new IntWritable();
        for (String word: words) {
            k.set(word);
            v.set(1);
            context.write(k,v); //构造器做轻量序列化

        }

    }
}
