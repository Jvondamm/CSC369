public class PairOfStringsAndInt
        implements Writable, WritableComparable<PairOfStringsAndInt> {

    private Text left=new Text();
    private Text right=new Text();
    private IntWritable num=new IntWritable();

    public PairOfStringsAndInt() {
    }
    public void set(Text left, Text right, IntWritable num){
        this.left = left;
        this.right = right;
        this.num = num;
    }
    
    public Text getLeftElement(){
        return left;
    }
    public Text getRightElement(){
        return right;
    }
    public Text getInt(){
        return num;
    }

    public PairOfStringsAndInt(Text left, Text right, IntWritable num) {
        this.left = left;
        this.right = right;
        this.num = num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        left.write(out);
        right.write(out);
        num.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        left.readFields(in);
        right.readFields(in);
        num.readFields(in);
    }
    @Override
    public int compareTo(PairOfStringsAndInt pair) {
        int compareValue = this.left.compareTo(pair.left);
        if (compareValue == 0) {
            compareValue = right.compareTo(pair.right);
        }
        return -1*compareValue;
    }
    public String toString(){
        return left+", "+right+", "+num;
    }
}