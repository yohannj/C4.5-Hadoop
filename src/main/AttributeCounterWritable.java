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
        if(res != 0) {
            return res;
        }
        
        res = classification.compareTo(o.classification);
        if(res != 0) {
            return res;
        }
        
        return count.compareTo(o.count);
    }

}
