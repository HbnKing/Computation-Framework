package teacher.anagram;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramMapper extends Mapper< Object, Text, Text, Text> {

    private Text sortedText = new Text();
    private Text orginalText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String word = value.toString();
        char[] wordChars = word.toCharArray();//单词转化为字符数组
        Arrays.sort(wordChars);//对字符数组按字母排序
        String sortedWord = new String(wordChars);//字符数组转化为字符串
        sortedText.set(sortedWord);//设置输出key的值
        orginalText.set(word);//设置输出value的值
        context.write( sortedText, orginalText );//map输出
    }

}