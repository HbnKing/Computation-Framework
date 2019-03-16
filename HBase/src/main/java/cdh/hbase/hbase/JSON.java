package cdh.hbase.hbase;
/*
{"id":[{"_id":"https://img.alicdn.com/imgextra/i4/1677335387/TB2MNEhiXXXXXXwXXXXXXXXXXXX_!!1677335387.jpg","width":"750","商品标题":"【努比亚旗舰店】nubia/努比亚 Z9 max 移动联通双4G版智能手机","商品链接":"https://detail.tmall.com/item.htm?id=45150572369","height":"1066"},
{"_id":"222","width":"750","商品标题":"【努比亚旗舰店】nubia/努比亚 Z9 max 移动联通双4G版智能手机","商品链接":"https://detail.tmall.com/item.htm?id=45150572369"}]}
*/
//json文件格式




import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


import com.google.gson.JsonArray;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
 
public class JSON {
private static Configuration conf = null;
    static {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "master");// 使用eclipse时必须添加这个，否则无法定位master需要配置hosts
            conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
    public static void main(String args[]) throws IOException{
    createTable("test","picture");
        JsonParser parse =new JsonParser();  //创建json解析器
        try {
        JsonParser parser=new JsonParser();  //创建JSON解析器
            JsonObject object=(JsonObject) parser.parse(new FileReader("E:\\qwe\\qwe.json"));  //创建JsonObject对象
            JsonArray array=object.get("id").getAsJsonArray(); 
            for(int i=0;i<array.size();i++){
                System.out.println("---------------");
                JsonObject subObject=array.get(i).getAsJsonObject();
                System.out.println("第"+(i+1)+"件商品");
           String a="null";
           String b="null";
           String c="null";
           String d="null";
                System.out.println("id="+subObject.get("_id").getAsString());
                if(subObject.get("width")!=null){System.out.println("width="+subObject.get("width").getAsInt());a=subObject.get("width").getAsString();}
                if(subObject.get("商品标题")!=null){System.out.println("商品标题="+subObject.get("商品标题").getAsString());b=subObject.get("商品标题").getAsString();}
                if(subObject.get("商品链接")!=null){System.out.println("商品链接="+subObject.get("商品链接").getAsString());c=subObject.get("商品链接").getAsString();}
                if(subObject.get("height")!=null){ System.out.println("height="+subObject.get("height").getAsInt());d=subObject.get("height").getAsString();}
                String[] cols = { "id","width","title","url","height"};//列 
                String[] colsValue = {subObject.get("_id").getAsString(),a,b,c,d};//值   
                String  str = String.valueOf(i+1);
                addData("test",str, cols, colsValue);//表名，行健，列，值
                
            }
            
            
        } catch (JsonIOException e) {
            e.printStackTrace();
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        
    }
    private static void addData(String tableName,String rowKey, String[] column1, String[] value1) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
        
                                                                                                                               // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                        .getColumnFamilies();


        for (int i = 0; i < columnFamilies.length; i++) {
                String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
                if (familyName.equals("picture")) { // article列族put数据
                        for (int j = 0; j < column1.length; j++) {
                                put.add(Bytes.toBytes(familyName),
                                                Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                        }
                }


        }
        table.put(put);
        System.out.println("add data Success!");
}
    private static void createTable(String TableName,String family) throws MasterNotRunningException,
    ZooKeeperConnectionException, IOException {


HBaseAdmin admin = new HBaseAdmin(conf);// 新建一个数据库管理员
if (admin.tableExists(TableName)) {
    System.out.println("table is exist!");
    System.exit(0);
} else {


    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TableName));
    desc.addFamily(new HColumnDescriptor(family));
    admin.createTable(desc);
    admin.close();
    System.out.println("create table Success!");
}
}
    
}
