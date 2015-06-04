package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AttributeInfoMapper extends MapReduceBase implements Mapper<TextArrayWritable, IntWritable, Text, AttributeCounterWritable> {

    public void map(TextArrayWritable key, IntWritable value, OutputCollector<Text, AttributeCounterWritable> output, Reporter reporter) throws IOException {

        output.collect(null, null);

    }
}
