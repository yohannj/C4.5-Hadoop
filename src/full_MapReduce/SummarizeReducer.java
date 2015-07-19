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
import org.apache.hadoop.mapreduce.Reducer;

public class SummarizeReducer extends Reducer<TextArrayWritable, IntWritable, TextArrayWritable, IntWritable> {

    public void reduce(TextArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for(IntWritable value : values) {
            count += value.get();
        }
        
        context.write(key, new IntWritable(count));
    }
}
