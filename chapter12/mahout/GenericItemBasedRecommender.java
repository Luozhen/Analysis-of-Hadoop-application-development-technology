package mahout;
import org.apache.mahout.cf.taste.impl.model.file.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.*;
import java.io.*;
import java.util.*;
public class GenericItemBasedRecommender  {
public static void main(String[] args) throws Exception{
  DataModel model;
model = new FileDataModel(new File("itembase.csv"));
ItemSimilarity itemsimilarity =new PearsonCorrelationSimilarity(model);
Recommender recommender= new GenericItemBasedRecommender(model,itemsimilarity);
List<RecommendedItem> recommendations =recommender.recommend(1, 1);
for(RecommendedItem recommendation :recommendations){
   System.out.println(recommendation);
}
}
}
