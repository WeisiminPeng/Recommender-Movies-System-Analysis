import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;

import java.io.*;

public class evaluate {

    public static void main(String[] args) throws Exception, TasteException {
        RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
//        RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
//        load HDFS dataset
        DataModel model = new FileDataModel(new File(
                "/Users/weisiminpeng/Desktop/7250/Assignments/FinalProject/ml-1m/ratingsModify.csv"));
        RecommenderBuilder builder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model) throws TasteException {
                UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
                UserNeighborhood neighborhood = new NearestNUserNeighborhood(6,
                        similarity, model);
                Recommender recommender = new GenericUserBasedRecommender(model,
                        neighborhood, similarity);
                return recommender;
            }
        };
        double score = evaluator.evaluate(builder, null, model, 0.8, 1.0);
        System.out.println(score);
    }
}

