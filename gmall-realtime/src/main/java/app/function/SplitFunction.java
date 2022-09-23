package app.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import utils.KeywordUtil;

import java.io.IOException;
import java.util.List;

/**
 * @author malichun
 * @create 2022/07/18 0018 23:42
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        try {
            // 分词
            List<String> strings = KeywordUtil.splitKeyWord(str);

            // 遍历并写出
            strings.forEach(s -> collect(Row.of(s)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
