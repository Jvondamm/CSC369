import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Stores
    implements Writable, WritableComparable<Stores> {
        private final Text Month = new Text();
        private final Text Name = new Text();
        private final IntWriteable Total = new IntWriteable();

    public Stores() {
    }

    public Stores(String Month, String Name, String Total) {
        this.Month.set(Month);
        this.Name.set(Name);
        this.Total.set(Total);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.Month.write(out);
        this.Name.write(out);
        this.Total.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.Month.readFields(in);
        this.Name.readFields(in);
        this.Total.readFields(in);
    }

    @Override
    public int compareTo(Stores other) {
        if(Month.compareTo(other.getMonth())==0){
            return Total.compareTo(other.Total);
        }
        return Month.compareTo(o.getMonth());
    }

    public Text getMonth() {
        return Month;
    }

    public Text getTotal() {
        return Total;
    }
    public Text getName() {
        return Name;
    }
}
