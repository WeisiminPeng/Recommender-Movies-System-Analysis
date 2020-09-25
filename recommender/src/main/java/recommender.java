import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import java.io.*;
import java.util.List;

public class recommender {

    public static void main(String[] args) throws IOException, TasteException{
//        load HDFS dataset
        DataModel model = new FileDataModel(new File(
                "/Users/weisiminpeng/Desktop/7250/Assignments/FinalProject/ml-1m/ratingsModifyTrain1.csv"));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new NearestNUserNeighborhood(6,
                similarity, model);
        Recommender recommender = new GenericUserBasedRecommender(model,
                neighborhood, similarity);
        PrintWriter writer = new PrintWriter(
                "/Users/weisiminpeng/Desktop/7250/Assignments/FinalProject/recommenderOutput/RecommenderTrainOutput1.csv");
        for (LongPrimitiveIterator iterator = model.getUserIDs(); iterator.hasNext(); ) {
            long userID = iterator.nextLong();
            List<RecommendedItem> recommendations = recommender.recommend(userID,
                    20);
            if (!recommendations.isEmpty()) {
                        System.out.println("UserID: " + userID);
                for (RecommendedItem recommendation : recommendations) {
                    StringBuilder rec = new StringBuilder();
                    rec.append(userID);
                    rec.append('%');
                    rec.append(recommendation.getItemID());
                    rec.append('%');
                    rec.append(recommendation.getValue());
                    rec.append('\n');
                    String stringRec = rec.toString();
                    if(stringRec.split("%")[2] != null) {
                        writer.write(rec.toString());
                        System.out.println("movieID: " + recommendation.getItemID()
                                + ",estimateRating: " + recommendation.getValue());
                    }
                }
            }
        }
    }
}


