package io.pravega.stream_shoveler;

import java.net.URI;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.internal.ParseContextImpl;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientConfig.ClientConfigBuilder;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.keycloak.client.PravegaKeycloakCredentials;

@ApplicationScoped
public class JsonShoveler {

	private static final Logger log = LoggerFactory.getLogger(JsonShoveler.class);

	@Inject
	@ConfigProperty(name = "stream-shoveler.from-controller-uri")
	Optional<String> fromController;

	@Inject
	@ConfigProperty(name = "stream-shoveler.from-keycloak-json")
	Optional<String> fromKeycloakJson;

	@Inject
	@ConfigProperty(name = "stream-shoveler.from-scope-name")
	String fromScopeName;

	@Inject
	@ConfigProperty(name = "stream-shoveler.from-stream-name")
	String fromStreamName;

	@Inject
	@ConfigProperty(name = "stream-shoveler.to-controller-uri")
	Optional<String> toController;

	@Inject
	@ConfigProperty(name = "stream-shoveler.to-keycloak-json")
	Optional<String> toKeycloakJson;

	@Inject
	@ConfigProperty(name = "stream-shoveler.create-to-scope", defaultValue = "true")
	boolean createToScope;

	@Inject
	@ConfigProperty(name = "stream-shoveler.to-scope-name")
	String toScopeName;

	@Inject
	@ConfigProperty(name = "stream-shoveler.create-to-stream", defaultValue = "true")
	boolean createToStream;

	@Inject
	@ConfigProperty(name = "stream-shoveler.to-stream-name")
	String toStreamName;

	@Inject
	@ConfigProperty(name = "stream-shoveler.routing-key-json-path")
	Optional<String> routingKeyJsonPath;

	public JsonShoveler() {
		initFromReader();
		initToWriter();
	}

	@PostConstruct
	void startup(/* @Observes StartupEvent se */) {
		log.info("================================================================================");
		log.info("Starting up JsonShoveler...");
		if (fromController.isPresent())
			log.info("stream-shoveler.from-controller-uri = {}", fromController.get());
		if (fromKeycloakJson.isPresent())
			log.info("stream-shoveler.from-keycloak-json = {}", fromKeycloakJson.get());
		log.info("stream-shoveler.from-scope-name = {}", fromScopeName);
		log.info("stream-shoveler.from-stream-name = {}", fromStreamName);
		if (toController.isPresent())
			log.info("stream-shoveler.to-controller-uri = {}", toController.get());
		if (toKeycloakJson.isPresent())
			log.info("stream-shoveler.to-keycloak-json = {}", toKeycloakJson.get());
		log.info("stream-shoveler.create-to-scope = {}", createToScope);
		log.info("stream-shoveler.to-scope-name = {}", toScopeName);
		log.info("stream-shoveler.create-to-stream = {}", createToStream);
		log.info("stream-shoveler.to-stream-name = {}", toStreamName);
		if (routingKeyJsonPath.isPresent())
			log.info("stream-shoveler.routing-key-json-path = {}", routingKeyJsonPath.get());
		log.info("================================================================================");
	}

	public void run() {
		Transaction<String> txn = writer.beginTxn();
		ParseContextImpl parseCxt = new ParseContextImpl();
		do {
			EventRead<String> event = null;
			try {
				event = reader.readNextEvent(2000);
				if (event.getEvent() != null) {
					String key = null;
					if (routingKeyJsonPath.isPresent()) {
						DocumentContext ctx = parseCxt.parse(event.getEvent());
						key = ctx.<String>read(routingKeyJsonPath.get()); // "$.peerAddress"
					}
					try {
						if (key != null) {
							txn.writeEvent(event.getEvent());
						} else {
							txn.writeEvent(key, event.getEvent());
						}
					} catch (TxnFailedException e) {
						// TODO FIXME restart from last checkpoint???
						// let's crash for now and see if that achieves the same result
						throw new RuntimeException(e);
					}
				} else if (event.isCheckpoint()) {
					try {
						txn.commit();
						txn = writer.beginTxn();
					} catch (TxnFailedException e) {
						// TODO FIXME restart from last checkpoint???
						// let's crash for now and see if that achieves the same result
						throw new RuntimeException(e);
					}
				}
			} catch (ReinitializationRequiredException e) {
				initFromReader();
			} catch (TruncatedDataException e) {
				log.warn("Read position was truncated. Skipping to next available event.");
			}
		} while (true);
	}

	private EventStreamReader<String> reader;

	private void initFromReader() {
		ClientConfigBuilder builder = ClientConfig.builder();

		if (fromController.isPresent()) {
			URI fromControllerURI = URI.create(fromController.get());
			builder.controllerURI(fromControllerURI);
		}

		if (toKeycloakJson.isPresent())
			builder.credentials(new PravegaKeycloakCredentials(toKeycloakJson.get()));
		ClientConfig clientConfig = builder.build();

		ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
				.stream(Stream.of(fromScopeName, fromStreamName))
				.build();

		String readerGroupName = "shoveler";
		String readerId = "0";
		try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(fromScopeName, clientConfig)) {
			readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);

			EventStreamClientFactory factory = EventStreamClientFactory.withScope(fromScopeName, clientConfig);
			ReaderConfig readerConfig = ReaderConfig.builder().build();
			try {
				reader = factory.createReader(readerId, readerGroupName, new UTF8StringSerializer(), readerConfig);
			} catch (IllegalStateException e) {
				readerGroupManager.getReaderGroup(readerGroupName).readerOffline(readerId, null);
				reader = factory.createReader(readerId, readerGroupName, new UTF8StringSerializer(), readerConfig);
			}
		}
	}

	private TransactionalEventStreamWriter<String> writer;

	private void initToWriter() {
		ClientConfigBuilder builder = ClientConfig.builder();
		if (toController.isPresent()) {
			URI toControllerURI = URI.create(toController.get());
			builder.controllerURI(toControllerURI);
		}
		if (toKeycloakJson.isPresent())
			builder.credentials(new PravegaKeycloakCredentials(toKeycloakJson.get()));
		ClientConfig clientConfig = builder.build();

		if (createToScope || createToStream)
			try (StreamManager streamManager = StreamManager.create(clientConfig)) {
				if (createToScope)
					streamManager.createScope(toScopeName);
				if (createToStream) {
					StreamConfiguration streamConfig = StreamConfiguration.builder()
							.scalingPolicy(ScalingPolicy.fixed(16))
							.build();
					streamManager.createStream(toScopeName, toStreamName, streamConfig);
				}
			}

		EventStreamClientFactory factory = EventStreamClientFactory.withScope(toScopeName, clientConfig);
		EventWriterConfig writerConfig = EventWriterConfig.builder().build();
		writer = factory.createTransactionalEventWriter(toStreamName, new UTF8StringSerializer(), writerConfig);
	}

}
