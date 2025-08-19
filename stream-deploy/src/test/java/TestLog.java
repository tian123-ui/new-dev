import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;


public class TestLog {
    private static final Logger LOG = LoggerFactory.getLogger(TestLog.class.getName());
    public static void main(String[] args) throws UnsupportedEncodingException {
        String encodedPassword = URLEncoder.encode("zh1028,./", "UTF-8");
        // 然后使用编码后的密码重新构建 URI，这里假设其他部分不变，仅替换密码部分
        String newUri = "s3://root:" + encodedPassword + "@10.39.48.35:9000/flk-data/ck";
        System.err.println(newUri);
    }
}
