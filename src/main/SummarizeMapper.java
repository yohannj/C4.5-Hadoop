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
