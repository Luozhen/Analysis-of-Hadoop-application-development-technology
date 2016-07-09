package mahout;
import java.util.List;
importorg.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
importorg.apache.mahout.cf.taste.model.DataModel;
importorg.apache.mahout.cf.taste.model.JDBCDataModel;
importorg.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
importorg.apache.mahout.cf.taste.recommender.RecommendedItem;
importorg.apache.mahout.cf.taste.recommender.Recommender;
importorg.apache.mahout.cf.taste.similarity.UserSimilarity;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
public class MahoutJdbcMysql {
public static  void main(String[] args) throws TasteException {
    // TODO Auto-generated method stub
long t1=System.currentTimeMillis();
MysqlDataSource dataSource=new MysqlDataSource(); ------------(1)
dataSource.setServerName("localhost");
dataSource.setUser("root");
dataSource.setPassword("root");
dataSource.setDatabaseName("mahoutDB");
JDBCDataModel dataModel=new MySQLJDBCDataModel(dataSource,"movieTable1",
"user_id","item_id","preference","time_date"); -----------(2)
DataModel model=dataModel;  ----------------(3)
UserSimilarity similarity=new PearsonCorrelationSimilarity(model);
UserNeighborhood neighborhood=new NearestNUserNeighborhood(2,similarity,model);
Recommender recommender=new GenericUserBasedRecommender(model,neighborhood,similarity);
List<RecommendedItem> recommendations=recommender.recommend(1, 2);
for(RecommendedItemrecommendation:recommendations){
System.out.println(recommendation);
        }
    }
}
