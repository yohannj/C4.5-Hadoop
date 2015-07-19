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
                res.put(new Text(value), new MapWritable());
            }
            MapWritable cur_map = (MapWritable) res.get(value);

            if (!cur_map.containsKey(classification)) {
                cur_map.put(new Text(classification), new IntWritable(0));
            }
            ((IntWritable) cur_map.get(classification)).set(((IntWritable) cur_map.get(classification)).get() + count.get());
        }

        context.write(key, res);
    }
}
