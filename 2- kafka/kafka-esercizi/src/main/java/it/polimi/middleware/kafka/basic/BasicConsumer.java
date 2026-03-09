package it.polimi.middleware.kafka.basic;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BasicConsumer {
    private static final String defaultGroupId = "groupA";
    private static final String defaultTopic = "topicA";

    private static final String serverAddr = "localhost:9092";
    /*
     * “Vuoi che Kafka salvi in automatico l’offset ogni tot secondi?”
     * 
     * Se autoCommit = true:
     * 
     * - Kafka aggiorna automaticamente l’offset del consumer
     * - non devi chiamare consumer.commitSync() o consumer.commitAsync()
     * - tutta la gestione degli offset è automatica
     * 
     * Tuttavia può essere pericoloso, perché se il consumer:
     * 1) legge un messaggio
     * 2) Kafka committa subito l’offset
     * 3) il consumer crasha prima di elaborare quel messaggio
     * 
     * -> il messaggio NON verrà più riletto
     * -> l’offset è già avanzato
     * 
     * Quindi c’è rischio di PERDERE messaggi.
     * 
     * Nota: auto-commit NON garantisce mai exactly-once.
     */
    /*
     * Se imposti autoCommit = false,
     * sei tu a gestire manualmente il salvataggio degli offset.
     * 
     * Esempio:
     * consumer.commitSync();
     * che va chiamato SOLO dopo aver processato correttamente i messaggi.
     * 
     * Questo è il metodo usato in produzione per evitare perdite o duplicati.
     * 
     * 👉 Per test o esempi didattici si usa invece:
     * autoCommit = true
     * che lascia a Kafka la gestione automatica degli offset.
     */
    private static final boolean autoCommit = true;
    // Ogni quanti millisecondi Kafka deve salvare l’offset
    private static final int autoCommitIntervalMs = 15000;

    // Default is "latest": try "earliest" instead
    /*
     * AUTO_OFFSET_RESET = "latest" significa:
     * "Se non ho un offset salvato, inizia a leggere SOLO dai messaggi nuovi,
     * quelli che arriveranno da ADESSO in poi."
     * 
     * Quindi:
     * - NON legge i messaggi già presenti nel topic
     * - NON legge lo storico
     * - legge soltanto i messaggi futuri
     * 
     * 🟥 Altra opzione: "earliest"
     * 
     * Significa:
     * "Se non ho un offset salvato, inizia a leggere dall'INIZIO del topic."
     * 
     * Quindi:
     * - legge TUTTI i messaggi presenti nel topic
     * - anche quelli vecchi
     * - anche quelli inviati ore o giorni fa
     * 
     * Utile quando vuoi rielaborare lo STORICO di un topic.
     */
    private static final String offsetResetStrategy = "latest";

    public static void main(String[] args) {
        // If there are arguments, use the first as group and the second as topic.
        // Otherwise, use default group and topic.
        String groupId = args.length >= 1 ? args[0] : defaultGroupId;
        String topic = args.length >= 2 ? args[1] : defaultTopic;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            /*
             * Effettua una "poll", cioè chiede a Kafka nuovi messaggi per il consumer.
             * 
             * consumer.poll(Duration.of(5, ChronoUnit.MINUTES))
             * 
             * - Attende fino a un massimo di 5 minuti in attesa di nuovi record.
             * - Se arrivano messaggi prima, li restituisce subito.
             * - Se non arriva niente, dopo 5 minuti ritorna comunque un oggetto
             * ConsumerRecords vuoto.
             * 
             * In pratica:
             * - legge i messaggi disponibili nelle partizioni assegnate
             * - oppure aspetta fino al timeout specificato (5 minuti)
             */
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, String> record : records) {
                System.out.print("Consumer group: " + groupId + "\t");
                System.out.println("Partition: " + record.partition() +
                        "\tOffset: " + record.offset() +
                        "\tKey: " + record.key() +
                        "\tValue: " + record.value());
            }
        }
    }
}