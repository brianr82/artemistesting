import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

public class ArtemisProducerExp {

    public static final Logger logger = Logger.getLogger(ArtemisProducerExp.class);

    public static void main(final String[] args) throws Exception {

        int number_of_producers = 4;

        for (int i=0; i < number_of_producers;i++){

            new ArtemisProducer("Producer : " + i , i).start();
        }


    }
}


class ArtemisProducer extends Thread{

    ClientProducer producer;
    ClientMessage test_operator_msg;
    ClientSession session;
    EventProfiler throughputProfiler;
    Integer ProducerID;

    public static final Logger logger = Logger.getLogger(ArtemisProducer.class);


    public ArtemisProducer(String name, Integer ProducerID) throws Exception {

        this.ProducerID = ProducerID;

        System.out.println("Starting producer: " + name);

        throughputProfiler = new EventProfiler(1000000, 1000000, 1, EventAggregationType.SUM);





        //ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
        ServerLocator locator = ActiveMQClient.createServerLocator("tcp://10.70.20.46:61616");

        locator.setConfirmationWindowSize(6400000);

        ClientSessionFactory factory =  locator.createSessionFactory();



        session = factory.createTransactedSession();
        session.setSendAcknowledgementHandler(new MySendAcknowledgementsHandler());




        System.out.println("Connected to Artemis Broker");


        String queueAddress  = "experiments";

        producer = session.createProducer(queueAddress);



        test_operator_msg = session.createMessage(true);




        test_operator_msg.getBodyBuffer().writeString("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

        System.out.println("Maxproducer amount: " + locator.getProducerMaxRate());




    }



    class MySendAcknowledgementsHandler implements SendAcknowledgementHandler {

        int count = 0;

        @Override
        public void sendAcknowledged(final Message message) {


            //System.out.println("Received send acknowledgement for message " + count++);
        }
    }



    @Override
    public void run() {

        PrintWriter writer = null;
        String filename =  "/home/brianr/artemis.exp.log." + currentThread().getName();
        try {
            writer = new PrintWriter(new FileOutputStream(new File(filename), false));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        /** Statistics* */
        long currentRecordcount = 0l;
        long windowStartTime  = System.nanoTime();
        long runningTotal =0l;
        long experimentStartTime = System.nanoTime();


        /** Batch size configuration*/
        int batchSize = 500;
        long desiredRuntime = 60000000000l * 10l;
        boolean isActive = true;
        int experimentAmount = 10000000;
        int cycleCount = experimentAmount/batchSize;


        int sequenceCounter = 0;
        System.out.println("Sending test tuples....");
        while(isActive){
        //for(int i=0; i<cycleCount;i++ ){


            try {
                int x ;
                for (x=0;x<= batchSize;x++){

                    //producer.send(test_operator_msg.setTimestamp(System.currentTimeMillis()));
                    producer.send(test_operator_msg);
                }


                session.commit();


                /*****************Log the throughput*******************************/
                long windowEndTime = System.nanoTime();
                long windowDuration = windowEndTime - windowStartTime;

                if (windowDuration > 1000000000l && (System.nanoTime() - experimentStartTime < 1000000000l * 300)) {

                    writer.println(" ProducerID, SeqNumber, WindowDuration, Throughput, RunningTotal: " + this.ProducerID + " " + sequenceCounter + " " + windowDuration +" " +  currentRecordcount +" " +  runningTotal);
                    
                    //reset the counter
                    currentRecordcount = 0 ;
                    windowStartTime = windowEndTime ;
                    sequenceCounter++;
                }else{

                    currentRecordcount = currentRecordcount + batchSize;
                }



                runningTotal = runningTotal + batchSize;
                /*********************************************************************/

                if(System.nanoTime() - experimentStartTime > desiredRuntime) {
                    isActive = false;
                }

            } catch (ActiveMQException e) {
                e.printStackTrace();
            }

        }


        try{

            session.close();

            //writer.flush();
            writer.close();

            long experimentEndTime = System.nanoTime();

            long experimenRunTime = experimentEndTime - experimentStartTime;

            System.out.println("Experiment Run time: " + experimenRunTime / 1000000000l);
            System.out.println("Throughput tuples/sec: " + runningTotal / (experimenRunTime / 1000000000l));
            //System.exit(0);
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }




    }
}


