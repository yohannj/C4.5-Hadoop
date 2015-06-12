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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AttributeGainRatioWritable implements WritableComparable<AttributeGainRatioWritable> {

    private Text name;
    private DoubleWritable gain_ratio;

    public AttributeGainRatioWritable() {
        set(new Text(), new DoubleWritable());
    }

    public AttributeGainRatioWritable(Text name, DoubleWritable gain_ratio) {
        set(name, gain_ratio);
    }

    public void set(Text name, DoubleWritable gain_ratio) {
        this.name = name;
        this.gain_ratio = gain_ratio;
    }

    public Text getname() {
        return name;
    }

    public DoubleWritable getGainRatio() {
        return gain_ratio;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        gain_ratio.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        gain_ratio.write(out);
    }

    @Override
    public int compareTo(AttributeGainRatioWritable o) {
        int res = name.compareTo(o.name);
        if (res != 0) {
            return res;
        }

        return gain_ratio.compareTo(o.gain_ratio);
    }

    @Override
    public String toString() {
        return name + "," + gain_ratio.get();
    }

}
