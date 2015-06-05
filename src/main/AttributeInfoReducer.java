package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AttributeInfoReducer extends Reducer<Text, AttributeCounterWritable, Text, MapWritable> {

    public void reduce(Text key, Iterable<AttributeCounterWritable> values, Context context) throws IOException, InterruptedException {

        MapWritable res = new MapWritable();
        Text value;
        Text classification;
        IntWritable count;

        for (AttributeCounterWritable cur_attribute_counter : values) {
            value = cur_attribute_counter.getValue();
            classification = cur_attribute_counter.getClassification();
            count = cur_attribute_counter.getCount();

            if (!res.containsKey(value)) {
                res.put(value, new MapWritable());
            }
            MapWritable cur_map = (MapWritable) res.get(value);

            if (!cur_map.containsKey(classification)) {
                cur_map.put(classification, new IntWritable(0));
            }
            ((IntWritable) cur_map.get(classification)).set(((IntWritable) cur_map.get(classification)).get() + count.get());
        }

        context.write(key, res);
    }
}
