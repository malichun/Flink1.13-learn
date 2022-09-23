package cn.doitedu.flink.java.exercise;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author malichun
 * @create 2022/08/31 0031 22:06
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventUserInfo {
    private int id;
    private String eventId;
    private int cnt;
    private String gender;
    private String city;
}
