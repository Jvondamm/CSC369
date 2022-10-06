import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

public class SecondarySortGroupingComparator
   extends WritableComparator {

    public SecondarySortGroupingComparator() {
        super(YMTemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1,
                                        WritableComparable wc2) {
        YMTemperaturePair pair = (YMTemperaturePair) wc1;
        YMTemperaturePair pair2 = (YMTemperaturePair) wc2;
        return pair.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
