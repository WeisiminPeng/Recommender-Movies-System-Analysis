import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {


    public CompositeKeyComparator(){
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyWritable c1 = (CompositeKeyWritable) a;
        CompositeKeyWritable c2 = (CompositeKeyWritable) b;

        int result = -1 * c1.getMovieid().compareTo(c2.getMovieid());
        return result;
    }
}
