import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TopNReducer  extends
   Reducer<NullWritable, Text, NullWritable, Text> {

   private int n = TopNMapper.DEFAULT_N;
   private SortedSet<Record> top = new TreeSet<>();

   @Override
   public void reduce(NullWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      for (Text value : values) {
         String valueAsString = value.toString().trim();
         String[] tokens = valueAsString.split(",");
         double weight =  Double.parseDouble(tokens[2]);
         top.add(new Record(Integer.parseInt(tokens[0]),
                                    tokens[1],weight));
         if (top.size() > n) {
            top.remove(top.last());
         }
      }
      
      for(Record r : top){
          context.write(NullWritable.get(), new Text(r.toString()));
      }
   }
   
   @Override
   protected void setup(Context context) 
      throws IOException, InterruptedException {
      this.n = context.getConfiguration().getInt("N", TopNMapper.DEFAULT_N);
   }
}