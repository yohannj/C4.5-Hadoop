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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class AttributeInfoMapper extends Mapper<TextArrayWritable, IntWritable, Text, AttributeCounterWritable> {

    public void map(TextArrayWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        Writable[] key_values = key.get();
        Text[] attributes_and_class = new Text[key_values.length];
        for (Integer i = 0; i < key_values.length; ++i) {
            String[] value_needed = conf.getStrings(i.toString());

            if (value_needed == null) {
                attributes_and_class[i] = (Text) key_values[i];
            } else if (!value_needed[0].equals(((Text) key_values[i]).toString())) {
                return;
            }
        }

        Text classification = attributes_and_class[attributes_and_class.length - 1];

        for (Integer i = 0; i < attributes_and_class.length - 1; ++i) {
            if (attributes_and_class[i] != null) {
                context.write(new Text(i.toString()), new AttributeCounterWritable(attributes_and_class[i], classification, value));
            }
        }

    }
}
