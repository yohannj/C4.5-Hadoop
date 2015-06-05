package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SummarizeReducer extends Reducer<TextArrayWritable, IntWritable, TextArrayWritable, IntWritable> {

    public void reduce(TextArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for(IntWritable value : values) {
            count += value.get();
        }
        
        context.write(key, new IntWritable(count));
    }
}
