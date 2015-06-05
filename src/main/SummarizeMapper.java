package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SummarizeMapper extends Mapper<LongWritable, Text, TextArrayWritable, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line_splitted = value.toString().split("\t");
        Text[] my_tmp_key = new Text[line_splitted.length];
        for (int i = 0; i < line_splitted.length; ++i) {
            my_tmp_key[i] = new Text(line_splitted[i]);
        }

        TextArrayWritable my_key = new TextArrayWritable();
        my_key.set(my_tmp_key);

        context.write(my_key, new IntWritable(1));
    }
}
