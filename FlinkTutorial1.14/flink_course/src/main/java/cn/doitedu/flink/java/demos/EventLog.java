package cn.doitedu.flink.java.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author malichun
 * @create 2022/08/28 0028 0:16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventLog {
    private long guid;
    private String sessionId;
    private String eventId;
    private long timestamp;
    private Map<String,String> eventInfo;
}