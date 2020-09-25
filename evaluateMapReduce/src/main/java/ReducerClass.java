import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<CompositeKeyWritable, CustomOutputTuple, CompositeKeyWritable, CustomOutputTuple> {

    CustomOutputTuple result = new CustomOutputTuple();
    private Double[] diff1 = new Double[]{0.0,0.0,0.0,0.0};

    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<CustomOutputTuple> values, Context context) throws IOException, InterruptedException {
//        initiate
        result.setAverageDiff(null);
        result.setRMS(null);
        result.setGenres(null);
        result.setTitle(null);
        result.setRating(0.0);
        result.setTestActionCount(0);
        result.setTestComedyCount(0);
        result.setTestDramaCount(0);
        result.setTestRomanceCount(0);
        result.setTestTotal(0);
        result.setTrainActionCount(0);
        result.setTrainComedyCount(0);
        result.setTrainDramaCount(0);
        result.setTrainRomanceCount(0);
        result.setTrainTotal(0);
        result.setType("none");

        for(CustomOutputTuple text : values){
//            System.out.println(text);
            if(text.getType().equals("train")){
//                System.out.println("111111111111111");
                result.setTrainComedyCount(result.getTrainComedyCount()+text.getTrainComedyCount());
//                System.out.println("%%%%%%%%%%%%%%%%%%%%%");
                result.setTrainActionCount(result.getTrainActionCount()+text.getTrainActionCount());
                result.setTrainDramaCount(result.getTrainDramaCount()+text.getTrainDramaCount());
                result.setTrainRomanceCount(result.getTrainComedyCount()+text.getTrainRomanceCount());
                result.setTrainTotal(result.getTrainTotal()+text.getTrainTotal()) ;
            } else if(text.getType().equals("test")){
//                System.out.println("222222222222");
//                listTest.add(text);
                result.setTestComedyCount(result.getTestComedyCount()+text.getTestComedyCount());
                result.setTestActionCount(result.getTestActionCount()+text.getTestActionCount());
                result.setTestDramaCount(result.getTestDramaCount()+text.getTestDramaCount());
                result.setTestRomanceCount(result.getTestComedyCount()+text.getTestRomanceCount());
                result.setTestTotal(result.getTestTotal()+text.getTestTotal()) ;
            }else if(text.getType().equals("none")){
//                System.out.println("222222222222");
//                listTest.add(text);
                result.setTestComedyCount(result.getTestComedyCount()+text.getTestComedyCount());
                result.setTestActionCount(result.getTestActionCount()+text.getTestActionCount());
                result.setTestDramaCount(result.getTestDramaCount()+text.getTestDramaCount());
                result.setTestRomanceCount(result.getTestComedyCount()+text.getTestRomanceCount());
                result.setTrainComedyCount(result.getTrainComedyCount()+text.getTrainComedyCount());
                result.setTrainActionCount(result.getTrainActionCount()+text.getTrainActionCount());
                result.setTrainDramaCount(result.getTrainDramaCount()+text.getTrainDramaCount());
                result.setTrainRomanceCount(result.getTrainComedyCount()+text.getTrainRomanceCount());
                result.setTestTotal(result.getTestTotal()+text.getTestTotal()) ;
                result.setTrainTotal(result.getTrainTotal()+text.getTrainTotal()) ;
            }
        }



//        calculate difference
        for(int i=0;i<diff1.length;i++){
            if(result.getTrainTotal() == 0 && result.getTestTotal() != 0){
                diff1[i] = new Double(Math.abs(0-(result.getTestComedyCount()/result.getTestTotal())));
            }
            if(result.getTrainTotal() != 0 && result.getTestTotal() == 0){
                diff1[i] = new Double(Math.abs((result.getTrainComedyCount()/result.getTrainTotal())));
            }
            if(result.getTrainTotal() == 0 && result.getTestTotal() == 0){
                diff1[i] = new Double(0.0);
            }
            if(result.getTrainTotal() != 0 && result.getTestTotal() != 0){
                diff1[i] = new Double(Math.abs((result.getTrainComedyCount()/result.getTrainTotal())-(result.getTestComedyCount()
                        /result.getTestTotal())));
            }
           }
//        calculate absolute average difference
        double sum = 0.0;
        for(int i=0;i<diff1.length;i++){
//            System.out.println(diff[i]);
            sum += diff1[i];
        }
//        System.out.println("*****************************");
        result.setAverageDiff(sum/4);

        double sumSquare = 0.0;
        for(int i=0;i<diff1.length;i++){
            sumSquare += diff1[i]*diff1[i];
        }
        result.setRMS(new Double(Math.sqrt(sumSquare/4)));

        context.write(key, result);
    }


}