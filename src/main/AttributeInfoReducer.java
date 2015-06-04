package main;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AttributeInfoReducer extends MapReduceBase implements Reducer<Text, AttributeCounterWritable, Text, MapWritable> {

    public void reduce(Text key, Iterator<AttributeCounterWritable> values, OutputCollector<Text, MapWritable> output, Reporter reporter) throws IOException {

        output.collect(null, null);
    }
}
