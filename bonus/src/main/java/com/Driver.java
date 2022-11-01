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

    /* JOB 2 */
    private static Path price; /* JOB 1 output */
    private static Path sales2;

    /* JOB 3 */
    private static Path store;
    private static Path sales3; /* JOB 2 output */

    /* JOB 4 */
    private static Path sales4; /* JOB 3 output */
    private static Path output;

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
          throw new IllegalArgumentException
                                    ("usage: <input1> <input2> <output>");
        }
         product = new Path(args[0]);
         lineItem = new Path(args[1]);
         sales2 = new Path(args[2]);
         price = new Path(args[3]);
         sales3 = new Path(args[4]);

        int returnStatus = ToolRunner.
                      run(new Driver(), args);
        THE_LOGGER.info("returnStatus=" + returnStatus);
        System.exit(returnStatus);
     }

    @Override
    public int run(String[] args) throws Exception {
      return (runJob1()&&runJob2())? 0 : 1;
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

        FileOutputFormat.setOutputPath(job1, price);
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

        FileOutputFormat.setOutputPath(job2, output);
        boolean status = job2.waitForCompletion(true);
        THE_LOGGER.info("run2(): status=" + status);
        return status;
    }
}