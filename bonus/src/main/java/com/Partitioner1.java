import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner1
        extends Partitioner<PairOfStrings, Object> {
    @Override
    public int getPartition(PairOfStrings key, 
                            Object value, 
                            int numberOfPartitions) {
       return Math.abs(key.getLeftElement().hashCode()) 
                                          % numberOfPartitions;
    }
}