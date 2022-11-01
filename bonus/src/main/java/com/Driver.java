import org.apache.log4j.Logger;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import java.io.IOException;

public class Driver extends Configured
                                         implements Tool {
    private static final Logger THE_LOGGER =
             Logger.getLogger(Driver.class);

    /* JOB 1 */
    private static Path product;
    private static Path lineItem;
    private static Path out1;

    /* JOB 2 */
    private static Path price;
    private static Path sales2;
    private static Path out2;

    /* JOB 3 */
    private static Path store;
    private static Path sales3;
    private static Path out3;

    /* JOB 4 */
    private static Path sales4;
    private static Path out4;

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
          throw new IllegalArgumentException
                                    ("usage: <input1> <input2> <input3> <input4> <output>");
        }
         product = new Path(args[0]);
         lineItem = new Path(args[1]);
         out1 = new Path(args[2]);

         price = new Path(args[3]); /* out1/part-r-00000 */
         sales2 = new Path(args[3]);
         out2 = new Path(args[4]);

         store = new Path(args[5]);
         sales3 = new Path(args[6]); /* out2/part-r-00000 */
         out3 = new Path(args[7]);

         sales4 = new Path(args[8]); /* out3/part-r-00000 */
         out4 = new Path(args[9]);

        int returnStatus = ToolRunner.run(new Driver(), args);
        THE_LOGGER.info("returnStatus=" + returnStatus);
        System.exit(returnStatus);
     }

    @Override
    public int run(String[] args) throws Exception {
      return (runJob1()&&runJob2()&&runJob3())? 0 : 1;
    }

    public boolean runJob1() throws IOException,
    InterruptedException, ClassNotFoundException {
        Job job1 = Job.getInstance();
        job1.setJarByClass(Driver.class);
        job1.setJobName("JOB 1");
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, lineItem,
        TextInputFormat.class, LineItemMapper1.class);
        MultipleInputs.addInputPath(job1, product,
          TextInputFormat.class, ProductMapper1.class);

        job1.setMapOutputKeyClass(PairOfStrings.class);
        job1.setMapOutputValueClass(PairOfStrings.class);
        job1.setReducerClass(Reducer1.class);
        job1.setPartitionerClass(Partitioner1.class);
        job1.setGroupingComparatorClass(GroupingComparator1.class);

        FileOutputFormat.setOutputPath(job1, out1);
        boolean status = job1.waitForCompletion(true);
        THE_LOGGER.info("run1(): status=" + status);
        return status;
    }

    public boolean runJob2() throws IOException,
    InterruptedException, ClassNotFoundException {
        Job job2 = Job.getInstance();
        job2.setJarByClass(Driver.class);
        job2.setJobName("JOB 2");
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, price,
        TextInputFormat.class, PriceMapper2.class);
        MultipleInputs.addInputPath(job2, sales2,
          TextInputFormat.class, SalesMapper2.class);

        job2.setMapOutputKeyClass(PairOfStrings.class);
        job2.setMapOutputValueClass(PairOfStrings.class);
        job2.setReducerClass(Reducer2.class);
        job2.setPartitionerClass(Partitioner1.class);
        job2.setGroupingComparatorClass(GroupingComparator1.class);

        FileOutputFormat.setOutputPath(job2, out2);
        boolean status = job2.waitForCompletion(true);
        THE_LOGGER.info("run2(): status=" + status);
        return status;
    }

    public boolean runJob3() throws IOException,
    InterruptedException, ClassNotFoundException {
        Job job3 = Job.getInstance();
        job3.setJarByClass(Driver.class);
        job3.setJobName("JOB 3");
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job3, sales3,
        TextInputFormat.class, SalesMapper3.class);
        MultipleInputs.addInputPath(job3, store,
          TextInputFormat.class, StoreMapper3.class);

        job3.setMapOutputKeyClass(PairOfStrings.class);
        job3.setMapOutputValueClass(PairOfStrings.class);
        job3.setReducerClass(Reducer2.class);
        job3.setPartitionerClass(Partitioner1.class);
        job3.setGroupingComparatorClass(GroupingComparator1.class);

        FileOutputFormat.setOutputPath(job3, out3);
        boolean status = job3.waitForCompletion(true);
        THE_LOGGER.info("run3(): status=" + status);
        return status;
    }
}