import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

public class SecondarySortGroupingComparator
   extends WritableComparator {

    public SecondarySortGroupingComparator() {
        super(Sales.class, true);
    }

    @Override
    public int compare(WritableComparable wc1,
                                        WritableComparable wc2) {
        Sales pair = (Sales) wc1;
        Sales pair2 = (Sales) wc2;
        return pair.getDate().compareTo(pair2.getDate());
    }
}
