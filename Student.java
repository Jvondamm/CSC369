public class SecondarySortPartitioner
   extends Partitioner<Student, Text> {

    @Override
    public int getPartition(Student pair,
                            Text Grade,
                            int numberOfPartitions) {
        return Math.abs(pair.Name.hashCode() % numberOfPartitions);
    }
}