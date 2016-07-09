package mahout;
importorg.apache.mahout.cf.taste.impl.model.file.*;
importorg.apache.mahout.cf.taste.impl.neighborhood.*;
importorg.apache.mahout.cf.taste.impl.recommender.*;
importorg.apache.mahout.cf.taste.impl.similarity.*;
importorg.apache.mahout.cf.taste.model.*;
importorg.apache.mahout.cf.taste.neighborhood.*;
importorg.apache.mahout.cf.taste.recommender.*;
importorg.apache.mahout.cf.taste.similarity.*;
import java.io.*;
importjava.util.*;

public class RecommenderClass{
	RecommenderClass () {
	}
	public static void main(String[] args) throws Exception {
		DataModel model = new FileDataModel(new File(	"useritem.txt"));
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(2,similarity, model);
		Recommender recommender = new GenericUserBasedRecommender(model,neighborhood, similarity);
		List<RecommendedItem> recommendations = recommender.recommend(1, 2);// 为用户1推荐两个ItemID
		for (RecommendedItem recommendation : recommendations) {
			System.out.println(recommendation);
		}
	}
}
