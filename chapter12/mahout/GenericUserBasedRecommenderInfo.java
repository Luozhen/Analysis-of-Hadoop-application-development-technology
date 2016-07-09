package mahout;
import org.apache.mahout.cf.taste.impl.model.file.*;
import org.apache.mahout.cf.taste.impl.neighborhood.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.*;
import java.io.*;
import java.util.*;
public class GenericUserBasedRecommenderInfo{
  public static void main(String[] args) throws Exception { 
    DataModel model = new FileDataModel(new File("userbase.csv"));  ----------(1)
    UserSimilarity similarity = new PearsonCorrelationSimilarity(model); ---------------(2)
    UserNeighborhood neighborhood =  new NearestNUserNeighborhood(2, similarity, model);
    Recommender recommender = new GenericUserBasedRecommender( model, neighborhood, similarity);  --------(3)
    List<RecommendedItem> recommendations = recommender.recommend(1, 1); -------------------(4)
    for (RecommendedItem recommendation : recommendations) { -------------(5)
         System.out.println(recommendation);
    }
  }
}
