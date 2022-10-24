import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;

public class SecondarySortReducer
    extends Reducer<Sales, Text, Text, Text> {

    @Override
    protected void reduce(Sales key,
                   Iterable<Text> values, Context context)
    	throws IOException, InterruptedException {
    	String result="";
    	for (Text value : values) {
            result += (value.toString()+",");
	}
       result = result.substring(0, result.length()-1);
       context.write(key.getDate(), new Text(result));
    }
}
