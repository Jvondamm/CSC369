import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;

public class PairOfStrings
        implements Writable, WritableComparable<PairOfStrings> {

    private Text left=new Text();
    private Text right=new Text();

    public PairOfStrings() {
    }
    public void set(Text left, Text right){
        this.left = left;
        this.right = right;
    }
    
    public Text getLeftElement(){
        return left;
    }
    public Text getRightElement(){
        return right;
    }

    public PairOfStrings(Text left, Text right) {
        this.left = left;
        this.right = right;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        left.write(out);
        right.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        left.readFields(in);
        right.readFields(in);
    }
    @Override
    public int compareTo(PairOfStrings pair) {
        int compareValue = this.left.compareTo(pair.left);
        if (compareValue == 0) {
            compareValue = right.compareTo(pair.right);
        }
        return -1*compareValue;
    }
    public String toString(){
        return left+", "+right;
    }
}