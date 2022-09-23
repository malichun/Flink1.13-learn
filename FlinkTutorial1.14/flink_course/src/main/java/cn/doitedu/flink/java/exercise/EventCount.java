package cn.doitedu.flink.java.exercise;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author malichun
 * @create 2022/08/31 0031 21:45
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCount {
    private int id;
    private String eventId;
    private int cnt;
}
