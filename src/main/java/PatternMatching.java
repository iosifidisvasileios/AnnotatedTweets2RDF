
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by iosifidis on 08.08.16.
 */
public class PatternMatching implements java.io.Serializable {

    public PatternMatching() {

    }
    public List<String> hasMore(String s){
        Pattern MY_PATTERN = Pattern.compile("#(\\w+)");
        Matcher mat = MY_PATTERN.matcher(s);
        List<String> strs = new ArrayList<String>();
        while (mat.find()) {
            //System.out.println(mat.group(1));
            strs.add(mat.group(1));
        }
//        System.out.println(strs);
        return strs;
    }
}
