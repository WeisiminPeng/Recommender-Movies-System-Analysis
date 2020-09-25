import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
    String userid;
    String movieid;
    public CompositeKeyWritable(){ }
    public CompositeKeyWritable(String userid, String movieid) {
        this.userid = userid;
        this.movieid = movieid;
    }
    public String getUserid() {
        return userid;
    }
    public void setUserid(String userid) {
        this.userid = userid;
    }
    public String getMovieid() {
        return movieid;
    }
    public void setMovieid(String movieid) {
        this.movieid = movieid;
    }
    public void readFields(DataInput in) throws IOException {
        userid = in.readUTF();
        movieid = in.readUTF();
    }
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userid);
        out.writeUTF(movieid);
    }
    public int compareTo(CompositeKeyWritable o) {
        int result = this.movieid.compareTo(o.getMovieid());
        return (result < 0 ? -1 : (result == 0 ? 0 : 1));
    }
    @Override
    public String toString() {
        return userid + "," + movieid;
    }
}
