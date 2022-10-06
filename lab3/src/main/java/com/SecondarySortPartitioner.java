import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner
                          // < key, value >
   extends Partitioner<Sales, IntWritable> {

    @Override
    public int getPartition(Sales Sale,
                            Text Time,
                            int numberOfPartitions) {
    return Math.abs(Sale.getDate().hashCode() %
                                           numberOfPartitions);
    }
}
