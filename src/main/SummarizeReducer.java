package main;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SummarizeReducer extends Reducer<TextArrayWritable, IntWritable, TextArrayWritable, IntWritable> {

    public void reduce(TextArrayWritable key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        while (values.hasNext()) {
            count += values.next().get();
        }
        
        context.write(key, new IntWritable(count));
    }
}
