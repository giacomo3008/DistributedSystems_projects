package it.polimi.middleware.kafka.admin;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/*
 Questo programma utilizza l'AdminClient di Kafka per gestire i topic.

 Funzionamento:

 1. Si connette al broker Kafka tramite AdminClient.
 2. Legge e stampa la lista dei topic esistenti.
 3. Se il topic specificato esiste già, lo cancella.
 4. Attende alcuni secondi per permettere al cluster di completare la cancellazione.
 5. Crea un nuovo topic con:
      - nome specificato
      - numero di partizioni desiderato
      - replication factor scelto
 6. Stampa una conferma quando l'operazione è completata.

 In pratica, questo è un “Topic Reset Script”:
 viene usato per eliminare e ricreare un topic da zero,
 utile durante test, esercizi o sviluppo locale.
*/

public class TopicManager {
    private static final String defaultTopicName = "topicA";
    private static final int defaultTopicPartitions = 2;
    /*
     * Replication Factor = numero di copie identiche di una partizione
     * mantenute sui vari broker del cluster Kafka.
     * 
     * Esempi:
     * - replication factor = 1 → nessuna replica (solo 1 copia)
     * - replication factor = 2 → 1 leader + 1 replica
     * - replication factor = 3 → 1 leader + 2 repliche
     * 
     * Significato pratico:
     * Ogni partizione del topic viene duplicata su broker diversi
     * per garantire tolleranza ai guasti.
     * Se il broker che contiene il leader va giù,
     * una replica può diventare il nuovo leader
     * senza perdita di dati.
     */
    // ESSENDO CHE STO USANDO SOLO IL MIO PC CON KAFKA CHE GIRA, HO UN SOLO BROKER
    // QUINDI NON POSSO AVERE IL REPLICATION FACTOR MAGGIORE DI 1!!!!!!

    // SE VUOI AUMENTARE LE REPLICHE DEVI AUMENTARE IL NUMERO DI AVVII KAFKA IN PIU
    // TEMRINALI!!!!
    private static final short defaultReplicationFactor = 1;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) throws Exception {
        final String topicName = args.length >= 1 ? args[0] : defaultTopicName;
        final int topicPartitions = args.length >= 2 ? Integer.parseInt(args[1]) : defaultTopicPartitions;
        final short replicationFactor = args.length >= 3 ? Short.parseShort(args[2]) : defaultReplicationFactor;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        AdminClient adminClient = AdminClient.create(props);

        ListTopicsResult listResult = adminClient.listTopics();
        Set<String> topicsNames = listResult.names().get();
        System.out.println("Available topics: " + topicsNames);

        if (topicsNames.contains(topicName)) {
            System.out.println("Deleting topic " + topicName);
            DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            delResult.all().get();
            System.out.println("Done!");
            // Wait for the deletion
            Thread.sleep(5000);
        }

        System.out.println("Adding topic " + topicName + " with " + topicPartitions + " partitions");
        NewTopic newTopic = new NewTopic(topicName, topicPartitions, replicationFactor);
        CreateTopicsResult createResult = adminClient.createTopics(Collections.singletonList(newTopic));
        createResult.all().get();
        System.out.println("Done!");
    }
}