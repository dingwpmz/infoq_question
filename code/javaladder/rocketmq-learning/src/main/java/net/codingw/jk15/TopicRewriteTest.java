package net.codingw.jk15;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.commons.lang3.StringUtils;

public class TopicRewriteTest {

    public static void main(String[] args) {


         String topic = "dw_test_topic";

         String color = System.getProperty("color", "");




    }


    public static String renameTopicName(String topicName) {
        String color = System.getProperty("color", "");
        if("BLUE".equals(color) && !topicName.startsWith("_BLUE_")) {
            return "_BLUE_" + topicName;
        } else if("GREEN".equals(color) && !topicName.startsWith("GREEN")) {
            return "GREEN" + topicName;
        }
        return topicName;
    }
}
