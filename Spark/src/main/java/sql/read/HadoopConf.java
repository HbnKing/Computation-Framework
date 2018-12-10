package sql.read;

import org.apache.hadoop.conf.Configuration;

public class HadoopConf {
    public Configuration getHadoopConf() {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hive-site.xml");
        //conf.addResource("yarn-site.xml");
        //conf.addResource("mapred-site.xml");
        return conf;
    }

}
