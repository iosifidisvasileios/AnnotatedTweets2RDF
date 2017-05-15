import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

/**
 * Created by iosifidis on 08.08.16.
 */
public class StopwordsLoader implements java.io.Serializable {
    static Logger logger = Logger.getLogger(StopwordsLoader.class);

    private final HashSet<String> dictionary = new HashSet();

    public StopwordsLoader() {
        FileInputStream fstream = null;
        try {
//            Path pt=new Path("stopwords.txt");
            Path pt=new Path("hdfs://nameservice1/user/iosifidis/stopwords.txt");

            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));

            String strLine;
            while ((strLine = br.readLine()) != null) {
                dictionary.add(strLine);
            }
        } catch (IOException e) {
            logger.error(e);
        }
    }
    public boolean isStopword(String word) throws  IOException{
        return dictionary.contains(word.toLowerCase());
    }

    public HashSet<String> getDictionary() {
        return dictionary;
    }
}
