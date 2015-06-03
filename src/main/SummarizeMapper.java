package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SummarizeMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TextArrayWritable, IntWritable> {

	public void map(LongWritable key, Text value,
			OutputCollector<TextArrayWritable, IntWritable> output,
			Reporter reporter) throws IOException {

		String[] line_splitted = value.toString().split("\t");
		Text[] my_tmp_key = new Text[line_splitted.length];
		for(int i = 0; i < line_splitted.length; ++i) {
			my_tmp_key[i] = new Text(line_splitted[i]);
		}
		
		TextArrayWritable my_key = new TextArrayWritable();
		my_key.set(my_tmp_key);

		output.collect(my_key, new IntWritable(1));

	}
}
