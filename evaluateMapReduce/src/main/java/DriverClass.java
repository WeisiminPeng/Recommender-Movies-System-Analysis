import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "finalprojectEvaluation");
        job.setJarByClass(DriverClass.class);

        job.setGroupingComparatorClass(NaturalGroupingKeyComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TrainMapperClass.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TestMapperClass.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(CustomOutputTuple.class);

        job.setReducerClass(ReducerClass.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path outDir = new Path(args[2]);
        TextOutputFormat.setOutputPath(job, outDir);

        job.setOutputKeyClass(CompositeKeyWritable.class);
        job.setOutputValueClass(CustomOutputTuple.class);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        System.exit(job.waitForCompletion(true)? 0:1);
//        job.waitForCompletion(true);
    }
}