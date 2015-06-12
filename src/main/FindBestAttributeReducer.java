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