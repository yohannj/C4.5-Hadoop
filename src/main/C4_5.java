package main;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class C4_5 {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: Weather <input path> <output path>");
			System.exit(-1);
		}

		JobConf conf = new JobConf(C4_5.class);
		conf.setJobName("C4.5");

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(SummarizeMapper.class);
		conf.setReducerClass(SummarizeReducer.class);

		conf.setOutputKeyClass(TextArrayWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		JobClient.runJob(conf);
	}

}
