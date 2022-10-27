import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;
import java.util.Iterator;

public class Reducer4
         extends Reducer<PairOfStrings, PairOfStrings, NullWritable, Text> {

   private int n = TopNMapper.DEFAULT_N;
   private SortedSet<Record> top = new TreeSet<>();
   private String date;

   public void reduce(NullWritable key, Iterable<Text> values, Context context) 
   throws IOException, InterruptedException {
      for (Text value : values) {
         String[] tokens = value.getRightElement.toString().trim().split(" ");
         date = tokens[0].trim();
         top.add(new Record(tokens[1].trim(), 
         tokens[2].trim(), Integer.parseInt(tokens[3].trim())));
         if (top.size() > n) {
            top.remove(top.last());
         }
      }
      String list = date;
      for(Record r : top){
         list += ", " + r.toString();
      }
      context.write(NullWritable.get(), new Text(list));
   }

    @Override
   protected void setup(Context context) 
      throws IOException, InterruptedException {
      this.n = context.getConfiguration().getInt("N", TopNMapper.DEFAULT_N);
   }
}