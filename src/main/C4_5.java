/**
 * This file is part of an implementation of C4.5 by Yohann Jardin.
 * 
 * This implementation of C4.5 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This implementation of C4.5 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this implementation of C4.5. If not, see <http://www.gnu.org/licenses/>.
 */

package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class C4_5 {

    private static String input_path;
    private static String summarized_data_path;
    private static String calc_attributes_info_path;
    private static String output_path;

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: main/C4_5 <input path> <tmp path> <output path>");
            System.exit(-1);
        }

        input_path = args[0];
        summarized_data_path = args[1] + "/summarized_data";
        calc_attributes_info_path = args[1] + "/calc_attributes_info";
        output_path = args[2];

        summarizeData();
        calcAttributesInfo();
        findBestAttribute();
    }

    private static void summarizeData() throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(C4_5.class);
        job.setJobName("C4.5_summarizeData");

        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(summarized_data_path));

        job.setMapperClass(SummarizeMapper.class);
        job.setReducerClass(SummarizeReducer.class);

        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
    }

    private static void calcAttributesInfo() throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(C4_5.class);
        job.setJobName("C4.5_calcAttributesInfo");

        FileInputFormat.addInputPath(job, new Path(summarized_data_path));
        FileOutputFormat.setOutputPath(job, new Path(calc_attributes_info_path));

        job.setMapperClass(AttributeInfoMapper.class);
        job.setReducerClass(AttributeInfoReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AttributeCounterWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
    }

    private static void findBestAttribute() throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(C4_5.class);
        job.setJobName("C4.5_findBestAttribute");

        FileInputFormat.addInputPath(job, new Path(calc_attributes_info_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        job.setMapperClass(FindBestAttributeMapper.class);
        job.setReducerClass(FindBestAttributeReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(AttributeGainRatioWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(AttributeGainRatioWritable.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
    }

}
