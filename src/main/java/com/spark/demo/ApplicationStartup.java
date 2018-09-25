package com.spark.demo;

import com.spark.demo.script.SparkService;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

@Component
@Log4j2
public class ApplicationStartup implements ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    private SparkService sparkService;

    @Override
    public void onApplicationEvent(@Nonnull ApplicationReadyEvent applicationReadyEvent) {
        log.info("---------onApplicationEvent start");
        sparkService.test();
        log.info("---------onApplicationEvent end");
    }


}
