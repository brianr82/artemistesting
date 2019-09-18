import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


public class ArtemisConsumerExp {

    public static void main(final String[] args) throws Exception {



        int number_of_consumers = Integer.parseInt(args[0]);

        for (int i=0; i < number_of_consumers;i++){

            new ArtemisConsumer("Consumer : " + i).start();
        }


    }
}


class ArtemisConsumer extends Thread{

    ClientConsumer consumer;

    ClientSession session;


    long experimentStartTime;

    long runningTotal =0l;

    String consumerName;

    //private static final Logger LOG = LoggerFactory.getLogger(ArtemisConsumer.class);

    public ArtemisConsumer(String name) throws Exception {


        this.consumerName = name;
        System.out.println("Starting consumer: " + this.consumerName);

        //ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
        ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");

        //locator.setConfirmationWindowSize(640000);
        locator.setConsumerWindowSize(500000);

        ClientSessionFactory factory =  locator.createSessionFactory();



        //session = factory.createTransactedSession();
        session = factory.createTransactedSession();

        try {
            session.start();
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }


        System.out.println("Connected to Artemis Broker");


        String queueAddress  = "input-queue-1";

        try {
            consumer = session.createConsumer(queueAddress);
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }
        try {
            consumer.setMessageHandler(new MyMessageHandler(this.consumerName));
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

        experimentStartTime = System.nanoTime();




    }


    class MyMessageHandler implements MessageHandler  {


        String consumerName;

        // Statistics
        long currentRecordcount = 0l;
        long windowStartTime  = System.nanoTime();

        public MyMessageHandler (String consumerName){

            this.consumerName = consumerName;
        }



        /////////////////////////////////

        int counter;

        @Override
        public void onMessage(ClientMessage clientMessage) {
        counter++;
        runningTotal++;

        try {
                clientMessage.acknowledge();
        } catch (ActiveMQException e) {
                e.printStackTrace();
        }
        if (counter>500){

            counter = 0;


            try {
                session.commit();


                //*****************Log the throughput******************************
                long windowEndTime = System.nanoTime();
                long windowDuration = windowEndTime - windowStartTime;

                if (windowDuration > 1000000000l && (System.nanoTime() - experimentStartTime < 1000000000l * 300)) {

                    //LOG.error("Time, Throughput, Total: {} {} {}", windowDuration, currentRecordcount,runningTotal);
                    System.out.println(this.consumerName + " received this many tuples in one second: "+ currentRecordcount + " The running total is: " + runningTotal + " Duration: " + windowDuration);
                    //reset the counter
                    currentRecordcount = 0 ;
                    windowStartTime = windowEndTime ;
                }
                else{

                    currentRecordcount = currentRecordcount + 500;
                }






            } catch (ActiveMQException e) {
                e.printStackTrace();
            }
        }

        }
    }


    public void run(){
        try {
            Thread.sleep(20000);

            session.close();


            long experimentEndTime = System.nanoTime();

            long experimentRunTime = experimentEndTime - experimentStartTime;

            System.out.println("Experiment Run time: " + experimentRunTime / 1000000000l);
            System.out.println("Throughput tuples/sec: " + runningTotal / (experimentRunTime / 1000000000l));




        } catch (InterruptedException | ActiveMQException e) {
            e.printStackTrace();
        }

    }

    /*
    @Override
    public void run() {

        // Statistics
        long currentRecordcount = 0l;
        long windowStartTime  = System.nanoTime();
        long runningTotal =0l;
        long experimentStartTime = System.nanoTime();


        while (true) {

            int exit =0;

            ClientMessage incoming =null;

            int batchsize = 500;
            int i =0;

            while(i < batchsize){
                //System.out.println("Calling receive");
                try {
                    incoming =  consumer.receive(1);



                } catch (ActiveMQException e) {
                    e.printStackTrace();
                }

                if (incoming == null) {
                    exit = 1 ;
                    break;
                }else {
                    try {
                        incoming.acknowledge();
                    } catch (ActiveMQException e) {
                        e.printStackTrace();
                    }
                    i++;
                }

            }


            try {
                session.commit();

                //*****************Log the throughput******************************
                long windowEndTime = System.nanoTime();
                long windowDuration = windowEndTime - windowStartTime;

                if (windowDuration > 1000000000l && (System.nanoTime() - experimentStartTime < 1000000000l * 300)) {

                    //LOG.error("Time, Throughput, Total: {} {} {}", windowDuration, currentRecordcount,runningTotal);

                    //reset the counter
                    currentRecordcount = 0 ;
                    windowStartTime = windowEndTime ;
                }
                else{

                    currentRecordcount = currentRecordcount + batchsize;
                }


                runningTotal = runningTotal + batchsize;
                ///////////

            } catch (ActiveMQException e) {
                e.printStackTrace();
            }


            if (exit ==1){
                System.out.println("No more tuples");
                break;
            }
        }

        try {
            session.close();

            long experimentEndTime = System.nanoTime();

            long experimentRunTime = experimentEndTime - experimentStartTime;

            System.out.println("Experiment Run time: " + experimentRunTime / 1000000000l);
            System.out.println("Throughput tuples/sec: " + runningTotal / (experimentRunTime / 1000000000l));


        } catch (ActiveMQException e) {
            e.printStackTrace();
        }
    }
    */



}
