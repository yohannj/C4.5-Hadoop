package main;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SummarizeReducer extends MapReduceBase implements
 Reducer<TextArrayWritable, IntWritable, TextArrayWritable, IntWritable> {

	public void reduce(TextArrayWritable key, Iterator<IntWritable> values,
			OutputCollector<TextArrayWritable, IntWritable> output, Reporter reporter)
			throws IOException {

		int count = 0;
		while (values.hasNext()) {
			count += values.next().get();
		}
		output.collect(key, new IntWritable(count));
	}
}
