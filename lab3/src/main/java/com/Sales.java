import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Sales
    implements Writable, WritableComparable<Sales> {
        private final Text Date = new Text();
        private final Text Time = new Text();
        private final Text SaleID = new Text();

    public Sales() {
    }

    public Sales(String Date, String Time, String SaleID) {
        this.Date.set(Date);
        this.Time.set(Time);
        this.SaleID.set(SaleID);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.Date.write(out);
        this.Time.write(out);
        this.SaleID.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.Date.readFields(in);
        this.Time.readFields(in);
        this.SaleID.readFields(in);
    }

    @Override
    public int compareTo(Sales o) {
        if(Date.compareTo(o.getDate())==0){
            return Time.compareTo(o.Time);
        }
        return Date.compareTo(o.getDate());
    }

    public Text getDate() {
        return Date;
    }

    public Text getSaleID() {
        return SaleID;
    }
    public Text getTime() {
        return Time;
    }
}
