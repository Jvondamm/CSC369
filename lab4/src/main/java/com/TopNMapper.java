import java.util.TreeSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.log4j.Logger;

public class TopNMapper extends
        Mapper<LongWritable, Text, NullWritable, Text> {
    public static final int DEFAULT_N = 10;
    private int n = DEFAULT_N;

    private TreeSet<Record> top = new TreeSet<>();

    private static final Logger THE_LOGGER =
             Logger.getLogger(TopNMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] tokens = line.split(",");
        THE_LOGGER.info(Arrays.toString(tokens));
        THE_LOGGER.info(tokens[2]);
        double weight = Double.parseDouble(tokens[2].trim());
        top.add(new Record(Integer.parseInt(tokens[0].trim()),tokens[1].trim(),weight));

        if (top.size() > n) {
            top.remove(top.last());
        }
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        this.n = context.getConfiguration().getInt("N", DEFAULT_N);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Record r : top) {
            context.write(NullWritable.get(), new Text(r.toString()));
        }
    }
}