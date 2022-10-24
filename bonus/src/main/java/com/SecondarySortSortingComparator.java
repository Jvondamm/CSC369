import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

public class SecondarySortSortingComparator
   extends WritableComparator {

    protected SecondarySortSortingComparator() {
        super(Sales.class, true);
    }

    @Override
    public int compare(WritableComparable wc1,
                                        WritableComparable wc2) {
        Sales pair = (Sales) wc1;
        Sales pair2 = (Sales) wc2;
        return pair.compareTo(pair2);
    }
}
