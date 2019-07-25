/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import sun.misc.BASE64Decoder;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
//
//        /*
//         * Instantiate with specified consumer group name.
//         */
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_redis_consumer");
//        consumer.setNamesrvAddr("127.0.0.1:9876");
//
//
//        /*
//         * Specify name server addresses.
//         * <p/>
//         *
//         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
//         * <pre>
//         * {@code
//         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
//         * }
//         * </pre>
//         */
//
//        /*
//         * Specify where to start in case the specified consumer group is a brand new one.
//         */
//        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//
//        /*
//         * Subscribe one more more topics to consume.
//         */
//        consumer.subscribe("0b6189567b7bf6f60dc13023868bba9a59677051", "*");
//
//        /*
//         *  Register callback to execute on arrival of messages fetched from brokers.
//         */
//        consumer.registerMessageListener(new MessageListenerConcurrently() {
//
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
//                ConsumeConcurrentlyContext context) {
////                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
//
//                for (MessageExt msg : msgs) {
//                    JSONObject jsonObject = JSONObject.parseObject(new String(msg.getBody()));
//                    JSONArray payload = jsonObject.getJSONArray("payload");
//                    for (Object o : payload) {
//                        byte[] decode = Base64.getDecoder().decode(o.toString());
//                        String s = new String(decode);
//                        System.out.println(s);
//                        System.out.println(JSONObject.parseObject(s));
//
//                    }
//
//                }
//
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });
//
//        /*
//         *  Launch the consumer instance.
//         */
//        consumer.start();
//
//        System.out.printf("Consumer Started.%n");

        final ByteBuffer test = ByteBuffer.allocate(40);
        test.putLong(1);
        test.putInt(1);
        test.putLong(1);
        test.putLong(2);
        test.putInt(2);
        test.putLong(2);
        System.out.println(test.position());
        test.flip();
        System.out.println(test.position());
        System.out.println(test.getLong());
        test.position(test.position() + 12);
        System.out.println(test.getLong());
    }
}
