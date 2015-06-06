package main;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindBestAttributeMapper extends Mapper<Text, MapWritable, NullWritable, AttributeGainRatioWritable> {

    public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {

    }
}
