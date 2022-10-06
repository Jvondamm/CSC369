import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner
                          // < key, value >
   extends Partitioner<YMTemperaturePair, IntWritable> {

    @Override
    public int getPartition(YMTemperaturePair pair,
                            IntWritable temperature,
                            int numberOfPartitions) {
    return Math.abs(pair.getYearMonth().hashCode() %
                                           numberOfPartitions);
    }
}
