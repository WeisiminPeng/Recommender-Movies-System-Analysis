import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrainMapperClass extends Mapper<LongWritable, Text, CompositeKeyWritable, CustomOutputTuple> {
    CustomOutputTuple result = new CustomOutputTuple();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] values = data.split("%");
        result.setTrainActionCount(0);
        result.setTrainComedyCount(0);
        result.setTrainDramaCount(0);
        result.setTrainRomanceCount(0);
        if (null != values && values.length > 4) {
//                        System.out.println(values[3]);
            if (Double.parseDouble(values[3]) == 5.0) {
                CompositeKeyWritable key1 = new CompositeKeyWritable(values[0], values[1]);
                String[] genres = values[4].split("\\|");
//                System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
//                System.out.println(values[4]);
                for(String genre : genres) {
                    if (genre != null) {
//                        System.out.println(genre);
                        if (genre.equals("Comedy")){
//                            System.out.println("11111111111111111111");
                            result.setTrainComedyCount(1);
                        }
                        if (genre.equals("Romance")) {
//                            System.out.println("22222222222222222222222");
                            result.setTrainRomanceCount(1);
                        }
                        if (genre.equals("Action")) {
//                            System.out.println("333333333333333333333");
                            result.setTrainActionCount(1);
                        }
                        if (genre.equals("Drama")) {
//                            System.out.println("4444444444444444");
                            result.setTrainDramaCount(0);
                        }
                    }
                }
//                System.out.println("^^^^^^^^^^^^^^^^");
                result.setTrainTotal(1);
                result.setType("train");
                result.setTitle(values[2]);
                result.setRating(Double.parseDouble(values[3]));

                result.setAverageDiff(0.0);
                result.setRMS(0.0);
                result.setGenres("none");
                result.setTestActionCount(0);
                result.setTestComedyCount(0);
                result.setTestDramaCount(0);
                result.setTestRomanceCount(0);
                result.setTestTotal(0);


                if(result.getTrainComedyCount()>0 || result.getTrainRomanceCount()>0 ||
                        result.getTrainActionCount()>0 && result.getTrainDramaCount()>0){
                    context.write(key1, result);
                }
                }
            }
        }
    }