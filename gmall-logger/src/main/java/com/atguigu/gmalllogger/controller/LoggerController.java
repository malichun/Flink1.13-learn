package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author malichun
 * @create 2022/07/09 0009 12:18
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @ResponseBody
    @GetMapping("test")
    public String test1(){
        System.out.println("success");
        return "success";
    }

    @GetMapping("test2")
    public String test2(@RequestParam("name") String nm,
                        @RequestParam(value = "age", defaultValue = "18") int age
                ){
        System.out.println(nm+":"+age);
        return "success";
    }

    @GetMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr){
        // 打印数据
        //System.out.println(jsonStr);
        // 将数据落盘
        log.info(jsonStr);
        // 将数据写入kafka
        kafkaTemplate.send("ods_base_log",jsonStr);
        return "success";
    }
}
