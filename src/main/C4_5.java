package main;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class C4_5 {

    private static String input_path;
    private static String summarized_data_path;
    private static String output_path;

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: main/C4_5 <input path> <tmp path> <output path>");
            System.exit(-1);
        }

        input_path = args[0];
        summarized_data_path = args[1] + "/summarized_data";
        output_path = args[2];

        summarizeData();
    }

    private static void summarizeData() throws IOException {
        JobConf conf = new JobConf(C4_5.class);
        conf.setJobName("C4.5");

        FileInputFormat.addInputPath(conf, new Path(input_path));
        FileOutputFormat.setOutputPath(conf, new Path(summarized_data_path));

        conf.setMapperClass(SummarizeMapper.class);
        conf.setReducerClass(SummarizeReducer.class);

        conf.setOutputKeyClass(TextArrayWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }

    private static void calcAttributesInfo(String[] conditions) throws IOException {
        JobConf conf = new JobConf(C4_5.class);
        conf.setJobName("C4.5");

        FileInputFormat.addInputPath(conf, new Path(summarized_data_path));
        FileOutputFormat.setOutputPath(conf, new Path(output_path));

        /*conf.setMapperClass(SummarizeMapper.class);
        conf.setReducerClass(SummarizeReducer.class);

        conf.setOutputKeyClass(TextArrayWritable.class);
        conf.setOutputValueClass(IntWritable.class);
        
        JobClient.runJob(conf);*/
    }

}
