import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

public class GroupingComparator1
    extends WritableComparator {

    protected GroupingComparator1() {
        super(PairOfStrings.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, 
                                        WritableComparable wc2) {
        PairOfStrings pair = (PairOfStrings) wc1;
        PairOfStrings pair2 = (PairOfStrings) wc2;
        return pair.getLeftElement().
                              compareTo(pair2.getLeftElement());
    }
}