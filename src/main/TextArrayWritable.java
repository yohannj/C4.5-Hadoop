package main;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TextArrayWritable extends ArrayWritable implements WritableComparable<TextArrayWritable> {

    public TextArrayWritable() {
        super(Text.class);
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
