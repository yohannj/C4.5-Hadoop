package main;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FindBestAttributeReducer extends Reducer<NullWritable, AttributeGainRatioWritable, NullWritable, AttributeGainRatioWritable> {

    public void reduce(NullWritable key, Iterable<AttributeGainRatioWritable> values, Context context) throws IOException, InterruptedException {
        AttributeGainRatioWritable best_attribute = null;
        DoubleWritable best_gain_ratio = new DoubleWritable(-Double.MAX_VALUE);

        for (AttributeGainRatioWritable value : values) {
            if (value.getGainRatio().compareTo(best_gain_ratio) > 0) {
                best_gain_ratio = value.getGainRatio();
                best_attribute = value;
            }
        }

        context.write(key, best_attribute);
    }
}