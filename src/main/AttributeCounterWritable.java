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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AttributeCounterWritable implements WritableComparable<AttributeCounterWritable> {

    private Text value;
    private Text classification;
    private IntWritable count;

    public AttributeCounterWritable() {
        set(new Text(), new Text(), new IntWritable());
    }

    public AttributeCounterWritable(Text value, Text classification, IntWritable count) {
        set(value, classification, count);
    }

    public void set(Text value, Text classification, IntWritable count) {
        this.value = value;
        this.classification = classification;
        this.count = count;
    }

    public Text getValue() {
        return value;
    }

    public Text getClassification() {
        return classification;
    }

    public IntWritable getCount() {
        return count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value.readFields(in);
        classification.readFields(in);
        count.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        value.write(out);
        classification.write(out);
        count.write(out);
    }

    @Override
    public int compareTo(AttributeCounterWritable o) {
        int res = value.compareTo(o.value);
        if (res != 0) {
            return res;
        }

        res = classification.compareTo(o.classification);
        if (res != 0) {
            return res;
        }

        return count.compareTo(o.count);
    }

}
