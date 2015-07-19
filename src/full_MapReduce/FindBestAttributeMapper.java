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

package full_MapReduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class FindBestAttributeMapper extends Mapper<Text, MapWritable, NullWritable, AttributeGainRatioWritable> {

    public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
        TextArrayWritable values = getValues(value);
        Map<Text, Integer> tuple_per_split = getTuplePerSplit(value);

        int tot_tuple = 0;
        for (Integer i : tuple_per_split.values()) {
            tot_tuple += i;
        }

        double global_entropy = global_entropy(value, tot_tuple);
        double gain = gain(global_entropy, tuple_per_split, value, tot_tuple);
        DoubleWritable gain_ratio = new DoubleWritable(gainRatio(gain, tuple_per_split, tot_tuple));

        context.write(NullWritable.get(), new AttributeGainRatioWritable(key, values, gain_ratio));
    }

    private TextArrayWritable getValues(MapWritable value) {
        TextArrayWritable res = new TextArrayWritable();
        Text[] tmp_res = new Text[value.keySet().size()];

        int index = 0;
        for (Writable w1 : value.keySet()) {
            MapWritable mw = (MapWritable) value.get(w1);
            int nb_class = mw.size();
            Text prefered_class = new Text();
            IntWritable best_count = new IntWritable(Integer.MIN_VALUE);
            for (Writable w2 : mw.keySet()) {
                if (((IntWritable) mw.get(w2)).compareTo(best_count) > 0) {
                    best_count = (IntWritable) mw.get(w2);
                    prefered_class.set((Text) w2);
                }
            }
            tmp_res[index++] = new Text(((Text) w1).toString() + " " + nb_class + " " + prefered_class.toString());
        }

        res.set(tmp_res);
        return res;
    }

    private Map<Text, Integer> getTuplePerSplit(MapWritable data) {
        Map<Text, Integer> res = new HashMap<Text, Integer>();

        Text my_text_key;
        int nb_tuple;
        for (Writable my_key : data.keySet()) {
            my_text_key = (Text) my_key;
            nb_tuple = 0;

            for (Writable my_value : ((MapWritable) data.get(my_key)).values()) {
                nb_tuple += ((IntWritable) my_value).get();
            }

            res.put(new Text(my_text_key), nb_tuple);
        }

        return res;
    }

    private double global_entropy(MapWritable data, int tot_tuple) {
        double res = 0.0;

        Map<Text, Integer> count_per_class = new HashMap<Text, Integer>();
        for (Writable tmp_cur_map : data.values()) {
            MapWritable cur_map = (MapWritable) tmp_cur_map;
            for (Writable cur_key : cur_map.keySet()) {
                Text cur_key_text = (Text) cur_key;
                if (!count_per_class.containsKey(cur_key_text)) {
                    count_per_class.put(new Text(cur_key_text), 0);
                }

                count_per_class.put(cur_key_text, ((IntWritable) cur_map.get(cur_key)).get() + count_per_class.get(cur_key_text));
            }
        }

        double p;
        for (Integer i : count_per_class.values()) {
            p = (i * 1.0) / tot_tuple;
            res -= p * Math.log(p) / Math.log(2);
        }

        return res;
    }

    private double gain(double global_entropy, Map<Text, Integer> tuple_per_split, MapWritable data, int tot_tuple) {
        double sum_partial_entropy = 0;

        Text my_text_key;
        int nb_tuple;
        double uniform_ratio;
        double p;
        for (Writable my_key : data.keySet()) {
            my_text_key = (Text) my_key;
            nb_tuple = tuple_per_split.get(my_text_key);
            uniform_ratio = (nb_tuple * 1.0) / tot_tuple;

            for (Writable my_count : ((MapWritable) data.get(my_key)).values()) {
                p = (((IntWritable) my_count).get() * 1.0) / nb_tuple;
                sum_partial_entropy -= uniform_ratio * p * Math.log(p) / Math.log(2);
            }

        }

        return global_entropy - sum_partial_entropy;
    }

    private double gainRatio(double gain, Map<Text, Integer> tuple_per_split, int tot_tuple) {
        double split_info = 0;

        double p;
        for (Integer i : tuple_per_split.values()) {
            p = (i * 1.0) / tot_tuple;
            split_info -= p * Math.log(p) / Math.log(2);
        }

        return gain / split_info;
    }
}
