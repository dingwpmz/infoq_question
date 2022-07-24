package net.codingw;

import org.apache.rocketmq.common.message.MessageDecoder;

public class MsgIDEncoderTest {

    public static void main(String[] args) throws Exception{

        System.out.println(MessageDecoder.decodeMessageId("0A07676100002A9F00000000155CBB74").getAddress());
        System.out.println(MessageDecoder.decodeMessageId("0A07676000002A9F0000000001342C7C").getAddress());






    }
}
