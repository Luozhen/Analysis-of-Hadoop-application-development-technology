package mahout;
import java.io.File;
import java.io.IOException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.MemoryDiffStorage;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.slopeone.DiffStorage;
import org.apache.mahout.common.RandomUtils;
public class SlopeOneRecommenderInfo{   
 public static void recommenderModelEvaluation(DataModel model) throws TasteException {       
   RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator(); 
   RandomUtils.useTestSeed();         
   RecommenderBuilder builder = new RecommenderBuilder() {   
   long diffStorageNb = 100000;
   @Override          
  public Recommender buildRecommender(DataModel dm) throws TasteException {             
   DiffStorage diffStorage = new MemoryDiffStorage(dm, Weighting.WEIGHTED, diffStorageNb);              
  return new SlopeOneRecommender(dm, Weighting.WEIGHTED, Weighting.WEIGHTED, diffStorage);
       }   
    };   
 }
 public static void main(String[] args) throws IOException, TasteException {   
  DataModel model = new FileDataModel(new File("data/dating/ratings.dat"));        
  recommenderModelEvaluation(model);  
  }
}
