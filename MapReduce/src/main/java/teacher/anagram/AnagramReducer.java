package teacher.anagram;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer extends Reducer< Text, Text, Text, Text> {
   
    private Text outputKey = new Text();
    private Text outputValue = new Text();

   
    public void reduce(Text anagramKey, Iterable< Text> anagramValues,
            Context context) throws IOException, InterruptedException {
            String output = "";
            //对相同字母组成的单词，使用 ~ 符号进行拼接
            for(Text anagam:anagramValues){
            	 if(!output.equals("")){
            		 output = output + "~" ;
            	 }
            	 output = output + anagam.toString() ;
            }
            StringTokenizer outputTokenizer = new StringTokenizer(output,"~" );
            //输出anagrams（字谜）大于2的结果
            if(outputTokenizer.countTokens()>=2)
            {
                    output = output.replace( "~", ",");
                    outputKey.set(anagramKey.toString());//设置key的值
                    outputValue.set(output);//设置value的值
                    context.write( outputKey, outputValue);//reduce
            }
    }

}
