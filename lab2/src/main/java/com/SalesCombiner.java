import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;

public class SalesCombiner
    extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, IntWritable value, Context context)
       throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable val: value){
           count++;
        }
       context.write(key, new IntWritable(count));
    }   
}