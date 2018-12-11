package org.hive.api;

import org.hive.QueryHiveJdbc;

/**
 * @author wangheng
 * @create 2018-12-10 下午6:55
 * @desc
 **/
public class Test {

    public static void main(String[] args) {
        QueryHiveJdbc  queryHiveJdbc = new QueryHiveJdbc();
        try {
            QueryHiveJdbc.execute("select count(1) from pointer2");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
