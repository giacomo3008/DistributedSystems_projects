package it.polimi.middleware.kafka.esercizi.uno;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class BasicProducer {
    private static final String defaultTopic = "topicA";

    private static final int numMessages = 100000;
    private static final int waitBetweenMsgs = 500;
    private static final boolean waitAck = false;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) {
        // If there are no arguments, publish to the default topic
        // Otherwise publish on the topics provided as argument
        List<String> topics = args.length < 1 ? Collections.singletonList(defaultTopic) : Arrays.asList(args);

        final Properties props = new Properties();
        // Producer, connettiti ai broker Kafka che si trovano a questo indirizzo
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        // di a KAFKA: Le KEY dei messaggi sono stringhe.Usale così come son.
        // Kafka invia byte alle partizioni, non oggetti Java.
        // Il serializer converte l’oggetto Java → byte[]
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Tutto ciò che va dentro Kafka deve essere serializzato in byte.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Sono i tipi della key e del value dei messaggi String, String
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        /*
         * NON ritorna un numero.
         * Ritorna un oggetto della classe java.util.Random.
         * 
         * È un oggetto che contiene:
         * - uno stato interno (un seed)
         * - un algoritmo per generare numeri pseudo-casuali
         * - metodi per generare numeri di vari tipi
         * 
         * Questo oggetto è come una “macchina per fare numeri casuali”.
         * int x = r.nextInt(10); genera un numero fra 0 e 9
         * int y = r.nextInt(); genera un intero qualunque
         */

        /*
         * 3) Perché usare Random è MEGLIO?
         * 
         * ▶ 1) Perché devi generare PIÙ numeri
         * 
         * Nel tuo programma generi:
         * - un topic casuale
         * - una key casuale
         * - ad ogni iterazione
         * - per 100.000 messaggi
         * 
         * Se usassi Math.random() ogni volta, sarebbe meno efficiente e meno leggibile,
         * ad esempio:
         * int k1 = (int)(Math.random() * 1000);
         * int k2 = (int)(Math.random() * 1000);
         * 
         * L'oggetto Random è progettato proprio per generare molti numeri consecutivi
         * in modo più semplice, veloce e pulito.
         */
        final Random r = new Random();

        for (int i = 0; i < numMessages; i++) {
            final String topic = topics.get(r.nextInt(topics.size()));
            final String key = "Key" + r.nextInt(1000);
            final String value = "Val" + i;
            System.out.println(
                    "Topic: " + topic +
                            "\tKey: " + key +
                            "\tValue: " + value);

            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            final Future<RecordMetadata> future = producer.send(record);

            if (waitAck) {
                try {
                    RecordMetadata ack = future.get();
                    System.out.println("Ack for topic " + ack.topic() + ", partition " + ack.partition() + ", offset "
                            + ack.offset());
                } catch (InterruptedException | ExecutionException e1) {
                    e1.printStackTrace();
                }
            }

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}