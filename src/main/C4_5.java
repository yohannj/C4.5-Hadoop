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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

    private static Path input_path;
    private static Path tmp_path;
    private static Path summarized_data_path;
    private static Path calc_attributes_info_path;
    private static Path output_path;

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: main/C4_5 <input path> <tmp path> <output path>");
            System.exit(-1);
        }

        input_path = new Path(args[0]);
        tmp_path = new Path(args[1]);
        summarized_data_path = new Path(args[1] + "/summarized_data");
        calc_attributes_info_path = new Path(args[1] + "/calc_attributes_info");
        output_path = new Path(args[2]);
        FileSystem fs = FileSystem.get(new Configuration());

        //Job which key result is a line of data and value is a counter
        summarizeData();

        Map<Map<String, String>, String> last_node_of = new HashMap<Map<String, String>, String>();
        Deque<Map<String, String>> conditions_to_test = new ArrayDeque<Map<String, String>>();

        Map<String, String> init = new HashMap<String, String>();
        last_node_of.put(init, null);
        conditions_to_test.add(init);

        while (!conditions_to_test.isEmpty()) {
            calcAttributesInfo(conditions_to_test.pop());
            findBestAttribute();

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(output_path + "/part-r-00000"))));
            String line;
            line = br.readLine();
            System.out.println(line);

            fs.delete(calc_attributes_info_path, true);
            fs.delete(output_path, true);
        }

        fs.delete(tmp_path, true);
    }

    private static void summarizeData() throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(C4_5.class);
        job.setJobName("C4.5_summarizeData");

        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, summarized_data_path);

        job.setMapperClass(SummarizeMapper.class);
        job.setReducerClass(SummarizeReducer.class);

        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
    }

    private static void calcAttributesInfo(Map<String, String> conditions) throws Exception {
        Configuration conf = new Configuration();
        for (Entry<String, String> condition : conditions.entrySet()) {
            conf.setStrings(condition.getKey(), condition.getValue());
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(C4_5.class);
        job.setJobName("C4.5_calcAttributesInfo");

        FileInputFormat.addInputPath(job, summarized_data_path);
        FileOutputFormat.setOutputPath(job, calc_attributes_info_path);

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

        FileInputFormat.addInputPath(job, calc_attributes_info_path);
        FileOutputFormat.setOutputPath(job, output_path);

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
