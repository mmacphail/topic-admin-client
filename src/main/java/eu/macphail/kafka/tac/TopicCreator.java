package eu.macphail.kafka.tac;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(name = "topic-creator", mixinStandardHelpOptions = true, version = "1.0",
        description = "Create topics in a Kafka Cluster")
public class TopicCreator implements Callable<Integer> {

    @Option(names = {"--bootstrap-server"},
            paramLabel = "<String: server to connect to>",
            required = true, description = "REQUIRED: The Kafka server to connect to")
    private String bootstrapServer;

    /*@Option(names = {"--file"},
            paramLabel = "<File>",
            required = true,
            description = "Path to a CSV file that contains the topic to create.\n" +
                    "The format of the file is the following:\n" +
                    "TOPIC_NAME;PARTITIONS;REPLICATION_FACTOR;properties separated by : in the form like min.insync.replicas=2\n" +
                    "The file takes no headers.")
    private File file;*/

    @Override
    public Integer call() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        AdminClient adminClient = AdminClient.create(props);
        ListTopicsResult topics = adminClient.listTopics();
        topics.names().get().forEach(System.out::println);
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TopicCreator()).execute(args);
        System.exit(exitCode);
    }
}
