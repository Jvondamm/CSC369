import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;

public class SecondarySortReducer
    extends Reducer<YMTemperaturePair, IntWritable, Text, Text> {

    @Override
    protected void reduce(YMTemperaturePair key,
                   Iterable<IntWritable> values, Context context)
    	throws IOException, InterruptedException {
    	String result="";
    	for (IntWritable value : values) {
            result += (value.toString()+",");
	}
       result = result.substring(0, result.length()-1);
       context.write(key.getYearMonth(), new Text(result));
    }
}
