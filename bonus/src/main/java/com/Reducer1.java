import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;

public class Reducer1
        extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {
    IntWritable Price = new IntWritable();
    IntWritable Quantity = new IntWritable();
    IntWritable SalesID = new IntWritable();
    Text Name = new Text();
    @Override
    public void reduce(PairOfStrings key, 
                   Iterable<PairOfStrings> values, Context context) 
       throws java.io.IOException, InterruptedException {
       Iterator<PairOfStrings> iterator = values.iterator();
       PairOfStrings firstPair = iterator.next(); 
       if (firstPair.getLeftElement().toString().equals("L")) {
         location.set(firstPair.getRightElement());
       } else {
         product.set(firstPair.getRightElement());
         context.write(product, new Text("undefined"));
         location = new Text("undefined");
       } 	 
       while (iterator.hasNext()) {
      PairOfStrings productPair = iterator.next(); 
         product.set(productPair.getRightElement());
         context.write(product, location);
       }
    }
}