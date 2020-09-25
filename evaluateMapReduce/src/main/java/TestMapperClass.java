import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapperClass extends Mapper<LongWritable, Text, CompositeKeyWritable, CustomOutputTuple> {

    CustomOutputTuple result = new CustomOutputTuple();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] values = data.split("%");

        result.setTestActionCount(0);
        result.setTestComedyCount(0);
        result.setTestDramaCount(0);
        result.setTestRomanceCount(0);

        if(null != values && values.length>4){
//            System.out.println(values[3]);
            if(Integer.parseInt(values[3]) == 5){
                double d = Integer.parseInt(values[3]);
                CompositeKeyWritable key1 = new CompositeKeyWritable(values[0], values[1]);
                String[] genres = values[4].split("\\|");
                for(String genre : genres) {
                    if (genre != null) {
                        if (genre.equals("Comedy")){
                            result.setTestComedyCount(1);
                        }
                        if (genre.equals("Romance")) {
                            result.setTestRomanceCount(1);
                        }
                        if (genre.equals("Action")) {
                            result.setTestActionCount(1);
                        }
                        if (genre.equals("Drama")) {
                            result.setTestDramaCount(0);
                        }
                    }
                }
                result.setTestTotal(1);
                result.setType("test");
                result.setTitle(values[2]);
                result.setRating(d);

                result.setAverageDiff(0.0);
                result.setRMS(0.0);
                result.setGenres("none");
                result.setTrainActionCount(0);
                result.setTrainComedyCount(0);
                result.setTrainDramaCount(0);
                result.setTrainRomanceCount(0);
                result.setTrainTotal(0);

                if(result.getTestComedyCount()>0 || result.getTestRomanceCount()>0 ||
                        result.getTestActionCount()>0 && result.getTestDramaCount()>0){
                    context.write(key1, result);
                }

            }
        }

    }
}