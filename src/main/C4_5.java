package main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class C4_5 {

    private static String input_path;
    private static String summarized_data_path;
    private static String output_path;

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: main/C4_5 <input path> <tmp path> <output path>");
            System.exit(-1);
        }

        input_path = args[0];
        summarized_data_path = args[1] + "/summarized_data";
        output_path = args[2];

        summarizeData();
        calcAttributesInfo();
    }

    private static void summarizeData() throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(C4_5.class);
        job.setJobName("C4.5_summarizeData");

        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(summarized_data_path));

        job.setMapperClass(SummarizeMapper.class);
        job.setReducerClass(SummarizeReducer.class);

        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }

    private static void calcAttributesInfo() throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(C4_5.class);
        job.setJobName("C4.5_calcAttributesInfo");

        FileInputFormat.addInputPath(job, new Path(summarized_data_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        job.setMapperClass(AttributeInfoMapper.class);
        job.setReducerClass(AttributeInfoReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.waitForCompletion(true);
    }

}
