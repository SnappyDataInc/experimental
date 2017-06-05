package io.snappydata.cassandra.readcdc;

import com.satori.rtm.*;
import com.satori.rtm.model.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class AirTrafficData {
    static final String endpoint = "wss://open-data.api.satori.com";
    static final String appkey = "B7D6dB9b78Df990792756DBD4ba36Ce6";//"6DE7460BB0FF9C0AA1EC5bCB837B6cfc"; //"830dbcc815C967CC994Dc9af0BaBCf56";//"B7D6dB9b78Df990792756DBD4ba36Ce6"; // 6DE7460BB0FF9C0AA1EC5bCB837B6cfc";
    static final String channel = "air-traffic";
    static Session session = null;

    public static void main(String[] args) throws Exception {

    String serverIP = "localhost";//"ec2-34-207-71-222.compute-1.amazonaws.com";
    String keyspace = "initkey";

    Cluster cluster = Cluster.builder()
     .addContactPoints(serverIP)
     .build();

    session = cluster.connect(keyspace);
    System.out.println("connected to cassandra cluster");
       final RtmClient client = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onEnterConnected(RtmClient client) {
                        System.out.println("Connected to RTM!");
                    }
                })
                .build();

        final CountDownLatch success = new CountDownLatch(1000000);

        SubscriptionListener listener = new SubscriptionAdapter() {
            @Override
            public void onSubscriptionData(SubscriptionData data) {
                for (AnyJson json : data.getMessages()) {
                   //System.out.println("insert into air_traffic_live JSON " + json);
                   try{
                     session.execute("insert into air_traffic_live4 JSON " +"'" +json+ "'");
                   
                   }catch(Exception e){
                     System.out.println("Got Exception "+ e);  
                     e.printStackTrace();
                   }

                }
                success.countDown();
            }
public void onCreated(){
                System.out.println("onCreated");
            }
            public void onDeleted(){ 
                System.out.println("onDeleted");
            }


            public void onEnterUnsubscribed(UnsubscribeRequest var1, UnsubscribeReply var2){
                System.out.println("onEnterUnsubscribed " + var2);
            }

            public void onLeaveUnsubscribed(UnsubscribeRequest var1, UnsubscribeReply var2){
                System.out.println("onLeaveUnsubscribed");
            }

            public void onEnterSubscribing(SubscribeRequest var1){
                System.out.println("onEnterSubscribing");
            }

            public void onLeaveSubscribing(SubscribeRequest var1){
                System.out.println("onLeaveSubscribing");
            }

            public void onEnterSubscribed(SubscribeRequest var1, SubscribeReply var2){
                System.out.println("onEnterSubscribed");
            }

            public void onLeaveSubscribed(SubscribeRequest var1, SubscribeReply var2){
                System.out.println("onLeaveSubscribed");
            }

            public void onEnterUnsubscribing(UnsubscribeRequest var1){
                System.out.println("onEnterUnsubscribing");
            }

            public void onLeaveUnsubscribing(UnsubscribeRequest var1){
                System.out.println("onEnterUnsubscribing");

            }

            public void onEnterFailed(){
                System.out.println("onEnterFailed");

            }

            public void onLeaveFailed(){
                System.out.println("onLeaveFailed");

            }

            public void onSubscriptionError(SubscriptionError var1){
                System.out.println("onSubscriptionError");

            }

            public void onSubscriptionInfo(SubscriptionInfo var1){
                System.out.println("onSubscriptionInfo");

            }
        };
  
        client.createSubscription(channel, SubscriptionMode.ADVANCED, listener);
        client.start();
        success.await(50000, TimeUnit.SECONDS);

        client.shutdown();
    }
}
