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

package minor_MapReduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TextArrayWritable extends ArrayWritable implements WritableComparable<TextArrayWritable> {

    public TextArrayWritable() {
        super(Text.class);
    }
    
    public TextArrayWritable(TextArrayWritable taw) {
        super(Text.class);
        set(taw.get().clone());
    }

    @Override
    public int compareTo(TextArrayWritable o) {
        int size_diff = this.get().length - o.get().length;
        if (size_diff != 0) {
            return size_diff;
        }

        for (int i = 0; i < this.get().length; ++i) {
            int res = ((Text) this.get()[i]).compareTo((Text) o.get()[i]);

            if (res != 0) {
                return res;
            }
        }

        return 0;
    }

    @Override
    public String toString() {
        Writable[] data = get();
        if (data.length == 0) {
            return "";
        }

        String res = data[0].toString();

        for (int i = 1; i < data.length; ++i) {
            res += "," + data[i].toString();
        }

        return res;
    }

}
