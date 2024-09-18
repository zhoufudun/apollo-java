package com.ctrip.framework.apollo.use.cases.spring.boot.apollo;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfig;
import com.ctrip.framework.apollo.spring.annotation.ApolloJsonValue;
import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication

@EnableApolloConfig(value = {"application", "OrderEntryAssignedRouteKeys", "OesSiteExecRptAssignedRouteKeys"})
public class Application {

    @ApolloJsonValue("key")
    private String value;

    @Value("key")
    private String key;

    @ApolloConfig("application")
//    @ApolloConfig("${namespace:application}")
    private Config config;


    @ApolloJsonValue("key")
    public void setValue(String key) {
        System.out.println(key);
    }

    @ApolloJsonValue("${student}")
    public void setValue3(Student student) {
        System.out.println("@ApolloJsonValue student="+student.toString());
    }

    @Value(value = "${key}")
    public void setValue2(String key) {
        System.out.println(key);
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Data
    static class Student{
        private String name;
        private int age;

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
