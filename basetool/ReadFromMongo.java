package basetool;

import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class ReadFromMongo {
	public static String[][] regexFromMongo(String DataBase,String CollectionName) {
		String[][] RegexArray = null;
		Mongo mg = null;
		
		try {
			mg = new Mongo();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		
		DB db = mg.getDB(DataBase);
		DBCollection collection = db.getCollection(CollectionName);
		// 指定查询条件
		DBObject query = new BasicDBObject();
		
		// 指定返回的键(字段)
		DBObject keys = new BasicDBObject();		
		keys.put("ID", 1);
		keys.put("REGEX", 1);  
		keys.put("_id", 0);
					
		DBCursor cur = collection.find(query,keys);
		List<DBObject> str = cur.toArray();
		RegexArray = new String[str.size()][2];
		
		//将regex,ID 存入二维数组
		for (int i = 0; i < str.size(); i++) {
			Object ID = str.get(i).get("ID");
			Object regex = str.get(i).get("REGEX");

			RegexArray[i][0] = regex.toString();
			RegexArray[i][1] = ID.toString();
		}
		return RegexArray;
	}
}
