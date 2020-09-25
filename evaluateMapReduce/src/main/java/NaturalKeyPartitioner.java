import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class NaturalKeyPartitioner extends Partitioner<CompositeKeyWritable, CustomOutputTuple> {

    @Override
    public int getPartition(CompositeKeyWritable key, CustomOutputTuple value, int noOfPartitions) {
        return key.getUserid().hashCode() % noOfPartitions;
//        return key.getUserid().hashCode();
    }
}
