package com.provectus.kafka.ui.serdes.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import com.provectus.kafka.ui.serde.api.PropertyResolver;
import com.provectus.kafka.ui.serde.api.Serde;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.ProtoFile;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaLoader;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.ResourceUtils;

class ProtobufFileSerdeTest {

  private static final String samplePersonMsgJson =
      "{ \"name\": \"My Name\",\"id\": 101, \"email\": \"user1@example.com\", \"phones\":[] }";

  private static final String sampleBookMsgJson = "{\"version\": 1, \"people\": ["
      + "{ \"name\": \"My Name\",\"id\": 102, \"email\": \"addrBook@example.com\", \"phones\":[]}]}";

  private static final String sampleSensorMsgJson = "{ \"name\": \"My Sensor\", "
      + "\"temperature\": 20.5, \"humidity\": 50, \"door\": \"OPEN\", \"lang\": \"EN\" }";

  // Sample message of type `test.Person`
  private byte[] personMessageBytes;
  // Sample message of type `test.AddressBook`
  private byte[] addressBookMessageBytes;
  private byte[] sensorMessageBytes;
  private Path addressBookSchemaPath;
  private Path sensorSchemaPath;
  private Descriptors.Descriptor personDescriptor;
  private Descriptors.Descriptor addressBookDescriptor;
  private Descriptors.Descriptor sensorDescriptor;
  private Map<Descriptors.Descriptor, Path> descriptorPaths;

  @BeforeEach
  void setUp() throws Exception {
    addressBookSchemaPath = ResourceUtils.getFile("classpath:protobuf-example/address-book.proto").toPath();
    sensorSchemaPath = ResourceUtils.getFile("classpath:iot/sensor.proto").toPath();

    SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
    schemaLoader.setLoadExhaustively(true);
    schemaLoader.setPermitPackageCycles(true);
    schemaLoader.initRoots(
        List.of(
            sourceLocation(addressBookSchemaPath),
            sourceLocation(sensorSchemaPath)
        ),
        Stream.of(
            protoLocation(addressBookSchemaPath),
            protoLocation(sensorSchemaPath)
        ).distinct().toList()
    );
    Schema schema = schemaLoader.loadSchema();
    Map<String, ProtoFileElement> dependencies =
        schema.getProtoFiles().stream().collect(Collectors.toMap(ProtoFile::toString, ProtoFile::toElement));

    ProtobufSchema addressBookSchema =
        new ProtobufSchema(schema.protoFile("address-book.proto").toElement(), List.of(), dependencies);
    var builder = addressBookSchema.newMessageBuilder("test.Person");
    JsonFormat.parser().merge(samplePersonMsgJson, builder);
    personMessageBytes = builder.build().toByteArray();

    builder = addressBookSchema.newMessageBuilder("test.AddressBook");
    JsonFormat.parser().merge(sampleBookMsgJson, builder);
    addressBookMessageBytes = builder.build().toByteArray();
    personDescriptor = addressBookSchema.toDescriptor("test.Person");
    addressBookDescriptor = addressBookSchema.toDescriptor("test.AddressBook");

    ProtobufSchema sensorSchema =
        new ProtobufSchema(schema.protoFile("sensor.proto").toElement(), List.of(), dependencies);
    builder = sensorSchema.newMessageBuilder("iot.Sensor");
    JsonFormat.parser().merge(sampleSensorMsgJson, builder);
    sensorMessageBytes = builder.build().toByteArray();
    sensorDescriptor = sensorSchema.toDescriptor("iot.Sensor");

    descriptorPaths = Map.of(
        personDescriptor, addressBookSchemaPath,
        addressBookDescriptor, addressBookSchemaPath,
        sensorDescriptor, sensorSchemaPath
    );
  }

  private Location sourceLocation(Path path) {
    return Location.get(path.getParent().toString(), path.getFileName().toString());
  }

  private Location protoLocation(Path path) {
    return Location.get(path.getParent().toString());
  }

  @Test
  void testDeserialize() {
    var messageNameMap = Map.of(
        "persons", personDescriptor,
        "books", addressBookDescriptor
    );
    var keyMessageNameMap = Map.of(
        "books", addressBookDescriptor);

    var serde = new ProtobufFileSerde();
    serde.configure(
        null,
        null,
        descriptorPaths,
        messageNameMap,
        keyMessageNameMap
    );

    var deserializedPerson = serde.deserializer("persons", Serde.Target.VALUE)
        .deserialize(null, personMessageBytes);
    assertJsonEquals(samplePersonMsgJson, deserializedPerson.getResult());

    var deserializedBook = serde.deserializer("books", Serde.Target.KEY)
        .deserialize(null, addressBookMessageBytes);
    assertJsonEquals(sampleBookMsgJson, deserializedBook.getResult());
  }

  @Test
  void testDeserializeMultipleProtobuf() {
    var messageNameMap = Map.of(
        "persons", personDescriptor,
        "books", addressBookDescriptor,
        "sensors", sensorDescriptor
    );
    var keyMessageNameMap = Map.of(
        "books", addressBookDescriptor);
    var serde = new ProtobufFileSerde();
    serde.configure(
        null,
        null,
        descriptorPaths,
        messageNameMap,
        keyMessageNameMap
    );

    var deserializedPerson = serde.deserializer("persons", Serde.Target.VALUE)
        .deserialize(null, personMessageBytes);
    assertJsonEquals(samplePersonMsgJson, deserializedPerson.getResult());

    var deserializedBook = serde.deserializer("books", Serde.Target.KEY)
        .deserialize(null, addressBookMessageBytes);
    assertJsonEquals(sampleBookMsgJson, deserializedBook.getResult());

    var deserializedSensor = serde.deserializer("sensors", Serde.Target.VALUE)
        .deserialize(null, sensorMessageBytes);
    assertJsonEquals(sampleSensorMsgJson, deserializedSensor.getResult());
  }

  @Test
  void testDefaultMessageName() {
    var serde = new ProtobufFileSerde();
    serde.configure(
        personDescriptor,
        addressBookDescriptor,
        descriptorPaths,
        Map.of(),
        Map.of()
    );

    var deserializedPerson = serde.deserializer("persons", Serde.Target.VALUE)
        .deserialize(null, personMessageBytes);
    assertJsonEquals(samplePersonMsgJson, deserializedPerson.getResult());

    var deserializedBook = serde.deserializer("books", Serde.Target.KEY)
        .deserialize(null, addressBookMessageBytes);
    assertJsonEquals(sampleBookMsgJson, deserializedBook.getResult());
  }

  @Test
  void testSerialize() {
    var messageNameMap = Map.of(
        "persons", personDescriptor,
        "books", addressBookDescriptor
    );
    var keyMessageNameMap = Map.of(
        "books", addressBookDescriptor);

    var serde = new ProtobufFileSerde();
    serde.configure(
        null,
        null,
        descriptorPaths,
        messageNameMap,
        keyMessageNameMap
    );

    var personBytes = serde.serializer("persons", Serde.Target.VALUE)
        .serialize("{ \"name\": \"My Name\",\"id\": 101, \"email\": \"user1@example.com\" }");
    assertThat(personBytes).isEqualTo(personMessageBytes);

    var booksBytes = serde.serializer("books", Serde.Target.KEY)
        .serialize("{\"version\": 1, \"people\": ["
            + "{ \"name\": \"My Name\",\"id\": 102, \"email\": \"addrBook@example.com\" }]}");
    assertThat(booksBytes).isEqualTo(addressBookMessageBytes);
  }

  @Test
  void testSerializeMultipleProtobuf() {
    var messageNameMap = Map.of(
        "persons", personDescriptor,
        "books", addressBookDescriptor,
        "sensors", sensorDescriptor
    );
    var keyMessageNameMap = Map.of(
        "books", addressBookDescriptor);

    var serde = new ProtobufFileSerde();
    serde.configure(
        null,
        null,
        descriptorPaths,
        messageNameMap,
        keyMessageNameMap
    );

    var personBytes = serde.serializer("persons", Serde.Target.VALUE)
        .serialize("{ \"name\": \"My Name\",\"id\": 101, \"email\": \"user1@example.com\" }");
    assertThat(personBytes).isEqualTo(personMessageBytes);

    var booksBytes = serde.serializer("books", Serde.Target.KEY)
        .serialize("{\"version\": 1, \"people\": ["
            + "{ \"name\": \"My Name\",\"id\": 102, \"email\": \"addrBook@example.com\" }]}");
    assertThat(booksBytes).isEqualTo(addressBookMessageBytes);

    var sensorBytes = serde.serializer("sensors", Serde.Target.VALUE)
        .serialize("{ \"name\": \"My Sensor\", \"temperature\": 20.5, \"humidity\": 50, "
            + "\"door\": \"OPEN\", \"lang\": \"EN\" }");
    assertThat(sensorBytes).isEqualTo(sensorMessageBytes);
  }

  @Test
  void testSerializeDefaults() {
    var serde = new ProtobufFileSerde();
    serde.configure(
        personDescriptor,
        addressBookDescriptor,
        descriptorPaths,
        Map.of(),
        Map.of()
    );

    var personBytes = serde.serializer("persons", Serde.Target.VALUE)
        .serialize("{ \"name\": \"My Name\",\"id\": 101, \"email\": \"user1@example.com\" }");
    assertThat(personBytes).isEqualTo(personMessageBytes);

    var booksBytes = serde.serializer("books", Serde.Target.KEY)
        .serialize("{\"version\": 1, \"people\": ["
            + "{ \"name\": \"My Name\",\"id\": 102, \"email\": \"addrBook@example.com\" }]}");
    assertThat(booksBytes).isEqualTo(addressBookMessageBytes);
  }

  @Test
  void canBeAutoConfiguredReturnsFalseIfNoProtoFilesHaveBeenProvided() {
    PropertyResolver resolver = mock(PropertyResolver.class);

    var serde = new ProtobufFileSerde();
    boolean startupSuccessful = serde.canBeAutoConfigured(resolver, resolver);
    assertThat(startupSuccessful).isFalse();
  }

  @Test
  void canBeAutoConfiguredReturnsFalseIfProtoFilesListIsEmpty() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getListProperty("protobufFiles", String.class)).thenReturn(Optional.of(List.of()));

    var serde = new ProtobufFileSerde();
    boolean startupSuccessful = serde.canBeAutoConfigured(resolver, resolver);
    assertThat(startupSuccessful).isFalse();
  }

  @Test
  void canBeAutoConfiguredReturnsTrueIfNoProtoFileHasBeenProvided() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getProperty("protobufFile", String.class)).thenReturn(Optional.of("file.proto"));

    var serde = new ProtobufFileSerde();
    boolean startupSuccessful = serde.canBeAutoConfigured(resolver, resolver);
    assertThat(startupSuccessful).isTrue();
  }

  @Test
  void canBeAutoConfiguredReturnsTrueIfProtoFilesHasBeenProvided() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getListProperty("protobufFiles", String.class)).thenReturn(Optional.of(List.of("file.proto")));

    var serde = new ProtobufFileSerde();
    boolean startupSuccessful = serde.canBeAutoConfigured(resolver, resolver);
    assertThat(startupSuccessful).isTrue();
  }

  @Test
  void canBeAutoConfiguredReturnsTrueIfProtoFileAndProtoFilesHaveBeenProvided() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getProperty("protobufFile", String.class)).thenReturn(Optional.of("file1.proto"));
    when(resolver.getListProperty("protobufFiles", String.class)).thenReturn(Optional.of(List.of("file2.proto")));

    var serde = new ProtobufFileSerde();
    boolean startupSuccessful = serde.canBeAutoConfigured(resolver, resolver);
    assertThat(startupSuccessful).isTrue();
  }

  @Test
  void listOfProtobufFilesIsJoined() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getProperty("protobufFile", String.class))
        .thenReturn(Optional.of(addressBookSchemaPath.toString()));
    when(resolver.getListProperty("protobufFiles", String.class))
        .thenReturn(Optional.of(List.of(sensorSchemaPath.getParent().toString())));
    when(resolver.getProperty("protobufMessageName", String.class))
        .thenReturn(Optional.of("test.AddressBook"));

    Map<String, String> protobufMessageNameByTopic = Map.of(
        "persons", "test.Person",
        "books", "test.AddressBook",
        "sensors", "iot.Sensor");
    when(resolver.getMapProperty("protobufMessageNameByTopic", String.class, String.class))
        .thenReturn(Optional.of(protobufMessageNameByTopic));

    var serde = new ProtobufFileSerde();
    serde.configure(resolver, resolver, resolver);

    var deserializedPerson = serde.deserializer("persons", Serde.Target.VALUE)
        .deserialize(null, personMessageBytes);
    assertJsonEquals(samplePersonMsgJson, deserializedPerson.getResult());

    var deserializedSensor = serde.deserializer("sensors", Serde.Target.VALUE)
        .deserialize(null, sensorMessageBytes);
    assertJsonEquals(sampleSensorMsgJson, deserializedSensor.getResult());
  }

  @Test
  void worksWithListOfExplicitDirectories() throws Exception {
    String sensorSchemaPath = ResourceUtils.getFile("classpath:iot-only-sensor").toString();
    String langSchemaPath = ResourceUtils.getFile("classpath:iot-only-lang").toString();

    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getListProperty("protobufFiles", String.class))
        .thenReturn(Optional.of(List.of(sensorSchemaPath, langSchemaPath)));
    when(resolver.getProperty("protobufMessageName", String.class))
        .thenReturn(Optional.of("iot.Sensor"));

    Map<String, String> protobufMessageNameByTopic = Map.of("sensors", "iot.Sensor");
    when(resolver.getMapProperty("protobufMessageNameByTopic", String.class, String.class))
        .thenReturn(Optional.of(protobufMessageNameByTopic));

    var serde = new ProtobufFileSerde();
    serde.configure(resolver, resolver, resolver);

    var deserializedSensor = serde.deserializer("sensors", Serde.Target.VALUE)
        .deserialize(null, sensorMessageBytes);
    assertJsonEquals(sampleSensorMsgJson, deserializedSensor.getResult());
  }

  @Test
  void unknownSchemaAsDefaultThrowsException() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getListProperty("protobufFiles", String.class))
        .thenReturn(Optional.of(List.of(addressBookSchemaPath.toString(), sensorSchemaPath.getParent().toString())));
    when(resolver.getProperty("protobufMessageName", String.class))
        .thenReturn(Optional.of("test.NotExistent"));

    var serde = new ProtobufFileSerde();
    assertThatThrownBy(() -> serde.configure(resolver, resolver, resolver))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("The given message type not found in protobuf definition: test.NotExistent");
  }

  @Test
  void unknownSchemaAsDefaultForKeyThrowsException() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getListProperty("protobufFiles", String.class))
        .thenReturn(Optional.of(List.of(addressBookSchemaPath.toString(), sensorSchemaPath.getParent().toString())));
    when(resolver.getProperty("protobufMessageName", String.class))
        .thenReturn(Optional.of("test.AddressBook"));
    when(resolver.getProperty("protobufMessageNameForKey", String.class))
        .thenReturn(Optional.of("test.NotExistent"));

    var serde = new ProtobufFileSerde();
    assertThatThrownBy(() -> serde.configure(resolver, resolver, resolver))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("The given message type not found in protobuf definition: test.NotExistent");
  }

  @Test
  void unknownSchemaAsTopicSchemaThrowsException() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getListProperty("protobufFiles", String.class))
        .thenReturn(Optional.of(List.of(addressBookSchemaPath.toString(), sensorSchemaPath.getParent().toString())));
    when(resolver.getProperty("protobufMessageName", String.class))
        .thenReturn(Optional.of("test.AddressBook"));

    when(resolver.getMapProperty("protobufMessageNameByTopic", String.class, String.class))
        .thenReturn(Optional.of(Map.of("persons", "test.NotExistent")));

    var serde = new ProtobufFileSerde();
    assertThatThrownBy(() -> serde.configure(resolver, resolver, resolver))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("The given message type not found in protobuf definition: test.NotExistent");
  }

  @Test
  void unknownSchemaAsTopicSchemaForKeyThrowsException() {
    PropertyResolver resolver = mock(PropertyResolver.class);
    when(resolver.getListProperty("protobufFiles", String.class))
        .thenReturn(Optional.of(List.of(addressBookSchemaPath.toString(), sensorSchemaPath.getParent().toString())));
    when(resolver.getProperty("protobufMessageName", String.class))
        .thenReturn(Optional.of("test.AddressBook"));

    when(resolver.getMapProperty("protobufMessageNameForKeyByTopic", String.class, String.class))
        .thenReturn(Optional.of(Map.of("persons", "test.NotExistent")));

    var serde = new ProtobufFileSerde();
    assertThatThrownBy(() -> serde.configure(resolver, resolver, resolver))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("The given message type not found in protobuf definition: test.NotExistent");
  }

  @SneakyThrows
  private void assertJsonEquals(String expectedJson, String actualJson) {
    var mapper = new JsonMapper();
    assertThat(mapper.readTree(actualJson)).isEqualTo(mapper.readTree(expectedJson));
  }
}
