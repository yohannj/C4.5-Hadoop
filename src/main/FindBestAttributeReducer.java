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

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FindBestAttributeReducer extends Reducer<NullWritable, AttributeGainRatioWritable, NullWritable, Text> {

    public void reduce(NullWritable key, Iterable<AttributeGainRatioWritable> values, Context context) throws IOException, InterruptedException {
        int nb_attributes_left = -1;
        TextArrayWritable DEBUG = new TextArrayWritable();
        DEBUG.set(new Text[]{new Text("ERROR")});
        AttributeGainRatioWritable best_attribute = new AttributeGainRatioWritable(new Text("ERROR"), DEBUG, new DoubleWritable(0.0));
        DoubleWritable best_gain_ratio = new DoubleWritable(-Double.MAX_VALUE);

        for (AttributeGainRatioWritable value : values) {
            ++nb_attributes_left;
            if (value.getGainRatio().compareTo(best_gain_ratio) > 0) {
                best_gain_ratio = new DoubleWritable(value.getGainRatio().get());
                best_attribute.set(new Text(value.getname()), new TextArrayWritable(value.getValues()), new DoubleWritable(value.getGainRatio().get()));
            }
        }

        context.write(key, new Text(best_attribute.toString() + "," + nb_attributes_left));
    }
}