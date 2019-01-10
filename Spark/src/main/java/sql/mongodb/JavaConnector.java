package sql.mongodb ;

import java.text.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.*;
import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import static com.mongodb.client.model.Filters.*;

/**
 * @author wangheng
 * @create 2018-09-18 下午2:16
 * @desc
 **/
public class JavaConnector {


    private static MongoDatabase mongoDatabase = null;
    private static ServerAddress serverAddress = null;
    private static MongoClient mongoClient  = null;
    private static MongoCredential  credentials  = null;
    private static List<ServerAddress> addressLists =new ArrayList<ServerAddress>();
    private static List<MongoCredential> credentialsLists = new ArrayList<MongoCredential>();


    public static void main(String args[]) throws ParseException {
            try {
                //未开启认证
                // 连接到 mongodb 服务
                //MongoClient mongoClient = new MongoClient("localhost", 27017);
                // 连接到数据库hello
                // 如果指定的数据库不存在，MongoDB会自动创建数据库



               // MongoDatabase mongoDatabase = mongoClient.getDatabase("authordatabase");
               // System.out.println("连接到数据库");


               // serverAddress = new ServerAddress("localhost",27017);
                //addressLists.add(serverAddress);
               // credentials = MongoCredential.createScramSha1Credential("username", "authordatabase", "password".toCharArray());
               // credentialsLists.add(credentials);
               // mongoClient = new MongoClient(addressLists, credentialsLists);

                String uriStr = "mongodb://192.168.3.131:27017/local.oplog.rs?replicaSet=wh";

                mongoClient = new MongoClient(new MongoClientURI(uriStr));



            }catch (MongoException e)
            {
                System.out.println(e.toString());
            }
            if(null != mongoClient){
                mongoDatabase = mongoClient.getDatabase("local");
                System.out.println("database mongoClient ");
           /* collection = database.getCollection("system.users");
           foundDocument = collection.find().into(
                    new ArrayList<Document>());
           System.out.println(foundDocument);*/
            }
                // 创建集合
                //mongoDatabase.createCollection("students");
                System.out.println("集合students创建成功");
                // 选择集合
                MongoCollection<Document> collection = mongoDatabase.getCollection("oplog.rs");
                System.out.println("集合students选择成功");
                // 插入文档
                /**
                 * 1. 创建文档 org.bson.Document 参数为key-value的格式 2. 创建文档集合List<Document>
                 * 3. 将文档集合插入数据库集合中 mongoCollection.insertMany(List<Document>)
                 * 插入单个文档可以用 mongoCollection.insertOne(Document)
                 */
               // Document document = new Document("name", "老司机5").append("age", 31);
               // List<Document> documents = new ArrayList<Document>();
                //documents.add(document);
                //collection.insertMany(documents);
                //System.out.println("文档插入成功");



                // 更新文档
                // 将文档中age=31的文档修改为age=32
                //collection.updateMany(eq("age", 51), new Document("$set", new Document("age", 77)));

               collection.find(
                        and(eq("status", "A"),
                                or(lt("qty", 30), regex("item", "^p")))
                );

               /* collection.find(
                        and(lte("insettime", ISODate("2017-12-31T16:00:50Z") ), gt("insettime", "^p"))
                );*/

                /**
                 * 数据筛选条件
                 *
                 */
                //BasicDBList condList = new BasicDBList();
                BasicDBObject cond1= new BasicDBObject();
                cond1.append("age",new BasicDBObject("$gt",40));
                //cond1.append("age",new BasicDBObject("$lte",77));
                //BasicDBObject cond2= new BasicDBObject();

                /**
                 *
                 */

                //System.out.println("文档更新成功");
                // 检索所有文档
                /**
                 * 1. 获取迭代器FindIterable<Document> 2. 获取游标MongoCursor<Document> 3.
                 * 通过游标遍历检索出的文档集合
                 */
                DateFormat format= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                //条件查找
                //FindIterable<Document> findIterable2 = collection.find(cond1);
                //全表
                FindIterable<Document> findIterable = collection.find();
                /*FindIterable<Document> findIterable =
                collection.find(
                        and(lt("insettime", format.parse("2018-01-04 07:00:10")), gte("insettime", format.parse("2018-01-04 07:00:00")))
                );*/
                //范围查找
                //FindIterable<Document> findIterable1 = collection.find({likes : {$age : 100});
                MongoCursor<Document> mongoCursor = findIterable.iterator();




                int counter =0  ;
                while (mongoCursor.hasNext()) {
                    System.out.println(mongoCursor.next()   +"   文档数据");
                    counter ++ ;
                }

        System.out.println(counter);

                // 删除符合条件的第一个文档
                //collection.deleteOne(Filters.eq("name", "老司机3"));
                // 删除所有符合条件的文档
               //collection.deleteMany(Filters.eq("name", "老司机"));
                // 关闭连接
                mongoClient.close();

        }



}
