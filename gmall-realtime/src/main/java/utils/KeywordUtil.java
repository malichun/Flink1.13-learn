package utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author malichun
 * @create 2022/07/18 0018 23:28
 */
public class KeywordUtil {

    public static List<String> splitKeyWord(String keyWord) throws IOException {
        // 创建集合用于存放结果数据
        ArrayList<String> resultList = new ArrayList<>();

        StringReader reader = new StringReader(keyWord);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        while (true) {
            Lexeme next = ikSegmenter.next();
            if (next != null) {
                String word = next.getLexemeText();
                resultList.add(word);
            }else {
                break;
            }
        }
        // 返回结果
        return resultList;
    }

    public static void main(String[] args) throws IOException {
        List<String> list = splitKeyWord("尚硅谷大数据项目之实时数仓");
        System.out.println(list);
    }
}
