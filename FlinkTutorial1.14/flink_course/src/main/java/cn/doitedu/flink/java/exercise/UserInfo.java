package cn.doitedu.flink.java.exercise;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author malichun
 * @create 2022/08/31 0031 21:46
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserInfo {
    private int id;
    private String gender;
    private String city;
}
