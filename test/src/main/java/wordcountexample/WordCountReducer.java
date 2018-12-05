package wordcountexample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by netswordof on 2018/11/18.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
       int sum =0;
        for (IntWritable count: values){
            sum += 1;
        }
        Text k = new Text();
        k.set(key);
        IntWritable v = new IntWritable(sum);
        context.write(k,v);
    }
}
