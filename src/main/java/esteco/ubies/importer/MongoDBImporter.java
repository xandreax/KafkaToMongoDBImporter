package esteco.ubies.importer;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import esteco.ubies.importer.propertiesHelper.PropertyReadHelper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * MongoDB Importer which consumes data elements from Kafka, converts them to MongoDB documents and adds them to the corresponding MongoDB collection.
 */
public class MongoDBImporter implements Runnable {

    /*
     * Flag to stop consuming from Kafka
     */
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    /*
     * kafka consumer
     */
    Consumer<Long, Document> kafkaConsumer;

    /*
     * id of registration received in the request
     */
    private final String uuid_registration;

    /*
     * name of the MongoDB collection of configuration data
     */
    private final String config_collection_name;

    /*
     * name of the MongoDB collection of matches data
     */
    private final String registration_collection_name;
    /*
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(MongoDBImporter.class);

    /*
     * Poll timeout
     */
    private static long pollTimeout;

    /*
     *  Name of the Kafka Topic
     * */
    private static String topic;

    /*
     * MongoDB database
     * */
    private final MongoDatabase database;

    /*
     * Broker list
     */
    private final String brokerList;

    /*
     * Group id
     * */
    private final String groupIdPrefix;

    /**
     * StreamImporter constructor.
     *
     * @param properties Properties
     */
    public MongoDBImporter(Properties properties, String uuid_registration) {
        logger.info("Read properties");
        pollTimeout = PropertyReadHelper.readLongOrDie(properties, "kafka.pollTimeout");
        brokerList = PropertyReadHelper.readStringOrDie(properties, "kafka.brokerList");
        groupIdPrefix = PropertyReadHelper.readStringOrDie(properties, "kafka.groupIdPrefix");
        String connectionString = PropertyReadHelper.readStringOrDie(properties, "mongodb.connectionString");
        String databaseName = PropertyReadHelper.readStringOrDie(properties, "mongodb.database");
        this.config_collection_name = PropertyReadHelper.readStringOrDie(properties, "mongodb.config_collection");
        this.registration_collection_name = PropertyReadHelper.readStringOrDie(properties, "mongodb.registration_collection");
        topic = PropertyReadHelper.readStringOrDie(properties, "kafka.topic");
        logger.info("Initializing KafkaConsumer");
        logger.info("KafkaConsumer initialized");
        logger.info("Initialize MongoDB");
        CodecRegistry msgCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoClientURI connectionURI = new MongoClientURI(connectionString);
        MongoDatabase database;
        MongoClient mongoClient = new MongoClient(connectionURI);
        database = mongoClient.getDatabase(databaseName);
        this.database = database.withCodecRegistry(msgCodecRegistry);
        logger.info("MongoDB initialized");
        this.uuid_registration = uuid_registration;
    }

    @Override
    public void run() {
        MongoCollection<Document> matchCollection = database.getCollection(this.registration_collection_name, Document.class);
        MongoCollection<Document> configCollection = database.getCollection(this.config_collection_name, Document.class);
        logger.info("Start consumption loop");
        KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory(brokerList, groupIdPrefix);
        kafkaConsumer = KafkaConsumerFactory.subscribe(topic);
        List<Document> registrationMessages = new LinkedList<>();
        Document registration_doc = new Document("registration_id", uuid_registration);
        try {
            Date time_start = null, time_interval = null;
            Document nested_msg = new Document();
            Date timestampToDate;
            boolean firstTimestampFound = false;
            while (!stopped.get()) {
                ConsumerRecords<Long, Document> records = kafkaConsumer.poll(Duration.ofMillis(pollTimeout));
                List<Document> configurationMessages = new LinkedList<>();
                for (ConsumerRecord<Long, Document> record : records) {
                    Document msg = record.value();
                    if (msg.containsKey("constellation")) {
                        nested_msg = msg.get("constellation", Document.class);
                        BsonTimestamp timestamp = nested_msg.get("timestamp",
                                BsonTimestamp.class);
                        timestampToDate = timestampToDate(timestamp);
                        nested_msg.append("timestamp", timestampToDate);
                        msg.append("registration_id", uuid_registration);
                        configurationMessages.add(msg);
                        System.out.println("constellation message added");
                    }
                    if (msg.containsKey("tag")) {
                        nested_msg = msg.get("tag", Document.class);
                    }
                    if (msg.containsKey("power")) {
                        nested_msg = msg.get("power", Document.class);
                    }
                    if (!nested_msg.isEmpty() && msg.containsKey("tag") || msg.containsKey("power")) {
                        BsonTimestamp timestamp = nested_msg.get("timestamp",
                                BsonTimestamp.class);
                        timestampToDate = timestampToDate(timestamp);
                        if (!firstTimestampFound) {
                            time_start = timestampToDate;
                            firstTimestampFound = true;
                            time_interval = new Date(time_start.getTime() + 1000);
                            registration_doc.append("timestamp", time_start);
                        }
                        if (timestampToDate.getTime() > time_interval.getTime()) {
                            registration_doc.append("messages", registrationMessages);
                            insertMany(matchCollection, Collections.singletonList(registration_doc));
                            time_start = time_interval;
                            time_interval = new Date(time_start.getTime() + 1000);
                            registrationMessages = new LinkedList<>();
                            registration_doc = new Document("registration_id", uuid_registration);
                            registration_doc.append("timestamp", time_start);
                        }
                        nested_msg.append("timestamp", timestampToDate);
                        registrationMessages.add(msg);
                    }
                }
                insertMany(configCollection, configurationMessages);
            }
        } catch (WakeupException we) {
            logger.info("wake up exception");
            if (!stopped.get())
                throw we;
        } finally {
            insertMany(matchCollection, Collections.singletonList(registration_doc));
            kafkaConsumer.close();
        }
    }

    public void stopImporter() {
        stopped.set(true);
        kafkaConsumer.wakeup();
        logger.info("STOP SENT");
    }

    /*
     * Inserts many documents into a MongoDB collection.
     *
     *  matchCollection collection where insert documents
     *  matchMessages   list of Network Messages
     */
    private void insertMany(MongoCollection<Document> collection, List<Document> messages) {
        try {
            if (!messages.isEmpty()) {
                collection.insertMany(messages);
                kafkaConsumer.commitSync();
                logger.info("Inserted " + messages.size() + " documents");
            }
        } catch (MongoWriteException e) {
            logger.info("Cannot insert due to MongoWriteException: ", e);
        } catch (MongoBulkWriteException e) {
            logger.info("Cannot insert due to MongoBulkWriteException: ", e);
        }
    }

    private Date timestampToDate(BsonTimestamp timestamp) {
        int seconds = timestamp.getTime();
        int inc = timestamp.getInc();

        //if timestamp precision is nanoseconds
        //long millisecond = ((seconds * 1000000L) + inc) / 1000;

        //if timestamp precision is millieseconds
        long millisecond = ((seconds * 1000L) + inc);

        return new Date(millisecond);
    }
}