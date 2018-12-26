package te;

import com.mongodb.ConnectionString;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author wangheng
 * @create 2018-12-11 下午6:31
 * @desc
 **/
public class Test {

    public static void main(String[] args) throws ParseException {
        DateFormat  dateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date  date = dateFormat.parse("2017-12-31 16:00:50");
        System.out.println(date);






    }
}
