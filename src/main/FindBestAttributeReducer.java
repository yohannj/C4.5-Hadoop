package main;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FindBestAttributeReducer extends Reducer<NullWritable, AttributeGainRatioWritable, NullWritable, AttributeGainRatioWritable> {

    public void reduce(Text key, Iterable<AttributeCounterWritable> values, Context context) throws IOException, InterruptedException {

    }
}
