/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.apache.pinot.broker.broker.BrokerServerBuilder;
import org.apache.pinot.broker.broker.BrokerTestUtils;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TableTaskConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Minion;
import org.apache.pinot.common.utils.CommonConstants.Server;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerRequestBuilderUtil;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.realtime.impl.kafka.AvroRecordToPinotRowGenerator;
import org.apache.pinot.core.realtime.impl.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfigProperties;
import org.apache.pinot.core.realtime.stream.StreamMessageDecoder;
import org.apache.pinot.core.util.AvroUtils;
import org.apache.pinot.minion.MinionStarter;
import org.apache.pinot.minion.events.MinionEventObserverFactory;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * Base class for integration tests that involve a complete Pinot cluster.
 */
public abstract class ClusterTest extends ControllerTest {
  private static final Random RANDOM = new Random();
  private static final int DEFAULT_BROKER_PORT = 18099;

  protected final String _clusterName = getHelixClusterName();
  protected String _brokerBaseApiUrl;

  private List<HelixBrokerStarter> _brokerStarters = new ArrayList<>();
  private List<HelixServerStarter> _serverStarters = new ArrayList<>();
  private List<MinionStarter> _minionStarters = new ArrayList<>();

  protected Schema _schema;
  protected TableConfig _offlineTableConfig;
  protected TableConfig _realtimeTableConfig;

  protected void startBroker() {
    startBrokers(1);
  }

  protected void startBroker(int basePort, String zkStr) {
    startBrokers(1, basePort, zkStr);
  }

  protected void startBrokers(int numBrokers) {
    startBrokers(numBrokers, DEFAULT_BROKER_PORT, ZkStarter.DEFAULT_ZK_STR);
  }

  protected void startBrokers(int numBrokers, int basePort, String zkStr) {
    _brokerBaseApiUrl = "http://localhost:" + basePort;
    for (int i = 0; i < numBrokers; i++) {
      Configuration configuration = BrokerTestUtils.getDefaultBrokerConfiguration();
      configuration.setProperty("pinot.broker.timeoutMs", 100 * 1000L);
      configuration.setProperty("pinot.broker.client.queryPort", Integer.toString(basePort + i));
      configuration.setProperty("pinot.broker.routing.table.builder.class", "random");
      configuration.setProperty(BrokerServerBuilder.DELAY_SHUTDOWN_TIME_MS_CONFIG, 0);
      // Randomly choose to use connection-pool or single-connection request handler
      if (RANDOM.nextBoolean()) {
        configuration.setProperty(BrokerServerBuilder.REQUEST_HANDLER_TYPE_CONFIG,
            BrokerServerBuilder.SINGLE_CONNECTION_REQUEST_HANDLER_TYPE);
      }
      overrideBrokerConf(configuration);
      _brokerStarters.add(BrokerTestUtils.startBroker(_clusterName, zkStr, configuration));
    }
  }

  public static Configuration getDefaultServerConfiguration() {
    Configuration configuration = DefaultHelixStarterServerConfig.loadDefaultServerConf();
    configuration.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, LOCAL_HOST);
    configuration.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
    configuration.addProperty(Server.CONFIG_OF_ENABLE_DEFAULT_COLUMNS, true);
    configuration.setProperty(Server.CONFIG_OF_ENABLE_SHUTDOWN_DELAY, false);
    return configuration;
  }

  protected void startServer() {
    startServers(1);
  }

  protected void startServer(Configuration configuration) {
    startServers(1, configuration, Server.DEFAULT_ADMIN_API_PORT, Helix.DEFAULT_SERVER_NETTY_PORT,
        ZkStarter.DEFAULT_ZK_STR);
  }

  protected void startServers(int numServers) {
    startServers(numServers, getDefaultServerConfiguration(), Server.DEFAULT_ADMIN_API_PORT,
        Helix.DEFAULT_SERVER_NETTY_PORT, ZkStarter.DEFAULT_ZK_STR);
  }

  protected void startServers(int numServers, int baseAdminApiPort, int baseNettyPort, String zkStr) {
    startServers(numServers, getDefaultServerConfiguration(), baseAdminApiPort, baseNettyPort, zkStr);
  }

  protected void startServers(int numServers, Configuration configuration, int baseAdminApiPort, int baseNettyPort,
      String zkStr) {
    try {
      for (int i = 0; i < numServers; i++) {
        configuration.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR, Server.DEFAULT_INSTANCE_DATA_DIR + "-" + i);
        configuration.setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
            Server.DEFAULT_INSTANCE_SEGMENT_TAR_DIR + "-" + i);
        configuration.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, baseAdminApiPort - i);
        configuration.setProperty(Server.CONFIG_OF_NETTY_PORT, baseNettyPort + i);
        // Set check interval time to 5 seconds for cluster tests.
        configuration.setProperty(Server.CONFIG_OF_INSTANCE_CHECK_INTERVAL_TIME, 5_000L);
        overrideServerConf(configuration);
        _serverStarters.add(new HelixServerStarter(_clusterName, zkStr, configuration));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void startMinion() {
    startMinions(1, null, null);
  }

  protected void startMinions(int minionCount,
      @Nullable Map<String, PinotTaskExecutorFactory> taskExecutorFactoryRegistry,
      @Nullable Map<String, MinionEventObserverFactory> eventObserverFactoryRegistry) {
    try {
      for (int i = 0; i < minionCount; i++) {
        Configuration config = new PropertiesConfiguration();
        config.setProperty(Helix.Instance.INSTANCE_ID_KEY,
            Minion.INSTANCE_PREFIX + "minion" + i + "_" + (Minion.DEFAULT_HELIX_PORT + i));
        config.setProperty(Helix.Instance.DATA_DIR_KEY, Minion.DEFAULT_INSTANCE_DATA_DIR + "-" + i);
        MinionStarter minionStarter = new MinionStarter(ZkStarter.DEFAULT_ZK_STR, _clusterName, config);

        // Register task executor factories
        if (taskExecutorFactoryRegistry != null) {
          for (Map.Entry<String, PinotTaskExecutorFactory> entry : taskExecutorFactoryRegistry.entrySet()) {
            minionStarter.registerTaskExecutorFactory(entry.getKey(), entry.getValue());
          }
        }

        // Register event observer factories
        if (eventObserverFactoryRegistry != null) {
          for (Map.Entry<String, MinionEventObserverFactory> entry : eventObserverFactoryRegistry.entrySet()) {
            minionStarter.registerEventObserverFactory(entry.getKey(), entry.getValue());
          }
        }

        minionStarter.start();
        _minionStarters.add(minionStarter);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void overrideServerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void overrideBrokerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void stopBroker() {
    for (HelixBrokerStarter brokerStarter : _brokerStarters) {
      BrokerTestUtils.stopBroker(brokerStarter);
    }
  }

  protected void stopServer() {
    for (HelixServerStarter helixServerStarter : _serverStarters) {
      helixServerStarter.stop();
    }
    FileUtils.deleteQuietly(new File(Server.DEFAULT_INSTANCE_BASE_DIR));
  }

  protected void stopMinion() {
    for (MinionStarter minionStarter : _minionStarters) {
      minionStarter.stop();
    }
    FileUtils.deleteQuietly(new File(Minion.DEFAULT_INSTANCE_BASE_DIR));
  }

  protected void addSchema(File schemaFile, String schemaName) throws Exception {
    if (!isUsingNewConfigFormat()) {
      try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
        fileUploadDownloadClient
            .addSchema(FileUploadDownloadClient.getUploadSchemaHttpURI(LOCAL_HOST, _controllerPort), schemaName,
                schemaFile);
      }
    } else {
      _schema = Schema.fromFile(schemaFile);
    }
  }

  /**
   * Upload all segments inside the given directory to the cluster.
   *
   * @param segmentDir Segment directory
   */
  protected void uploadSegments(@Nonnull File segmentDir) throws Exception {
    String[] segmentNames = segmentDir.list();
    Assert.assertNotNull(segmentNames);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      final URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentHttpURI(LOCAL_HOST, _controllerPort);

      // Upload all segments in parallel
      int numSegments = segmentNames.length;
      ExecutorService executor = Executors.newFixedThreadPool(numSegments);
      List<Future<Integer>> tasks = new ArrayList<>(numSegments);
      for (final String segmentName : segmentNames) {
        final File segmentFile = new File(segmentDir, segmentName);
        tasks.add(executor.submit(new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentName, segmentFile)
                .getStatusCode();
          }
        }));
      }
      for (Future<Integer> task : tasks) {
        Assert.assertEquals((int) task.get(), HttpStatus.SC_OK);
      }
      executor.shutdown();
    }
  }

  protected void addOfflineTable(String tableName) throws Exception {
    addOfflineTable(tableName, SegmentVersion.v1);
  }

  protected void addOfflineTable(String tableName, SegmentVersion segmentVersion) throws Exception {
    addOfflineTable(tableName, null, null, null, null, null, segmentVersion, null, null, null);
  }

  protected void addOfflineTable(String tableName, String timeColumnName, String timeType, String brokerTenant,
      String serverTenant, String loadMode, SegmentVersion segmentVersion, List<String> invertedIndexColumns,
      List<String> bloomFilterColumns, TableTaskConfig taskConfig) throws Exception {
    TableConfig tableConfig =
        getOfflineTableConfig(tableName, timeColumnName, timeType, brokerTenant, serverTenant, loadMode, segmentVersion,
            invertedIndexColumns, bloomFilterColumns, taskConfig);

    if (!isUsingNewConfigFormat()) {
      sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJSONConfigString());
    } else {
      _offlineTableConfig = tableConfig;
    }
  }

  protected void updateOfflineTable(String tableName, String timeColumnName, String timeType, String brokerTenant,
      String serverTenant, String loadMode, SegmentVersion segmentVersion, List<String> invertedIndexColumns,
      List<String> bloomFilterColumns, TableTaskConfig taskConfig) throws Exception {
    TableConfig tableConfig =
        getOfflineTableConfig(tableName, timeColumnName, timeType, brokerTenant, serverTenant, loadMode, segmentVersion,
            invertedIndexColumns, bloomFilterColumns, taskConfig);

    if (!isUsingNewConfigFormat()) {
      sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJSONConfigString());
    } else {
      _offlineTableConfig = tableConfig;
    }
  }

  private static TableConfig getOfflineTableConfig(String tableName, String timeColumnName, String timeType,
      String brokerTenant, String serverTenant, String loadMode, SegmentVersion segmentVersion,
      List<String> invertedIndexColumns, List<String> bloomFilterColumns, TableTaskConfig taskConfig) throws Exception {
    return new TableConfig.Builder(Helix.TableType.OFFLINE).setTableName(tableName)
        .setTimeColumnName(timeColumnName)
        .setTimeType(timeType)
        .setNumReplicas(3)
        .setBrokerTenant(brokerTenant)
        .setServerTenant(serverTenant)
        .setLoadMode(loadMode)
        .setSegmentVersion(segmentVersion.toString())
        .setInvertedIndexColumns(invertedIndexColumns)
        .setBloomFilterColumns(bloomFilterColumns)
        .setTaskConfig(taskConfig)
        .build();
  }

  protected void dropOfflineTable(String tableName) throws Exception {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
  }

  protected boolean isUsingNewConfigFormat() {
    return false;
  }

  public static class AvroFileSchemaKafkaAvroMessageDecoder implements StreamMessageDecoder<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileSchemaKafkaAvroMessageDecoder.class);
    public static File avroFile;
    private org.apache.avro.Schema _avroSchema;
    private AvroRecordToPinotRowGenerator _rowGenerator;
    private DecoderFactory _decoderFactory = new DecoderFactory();
    private DatumReader<GenericData.Record> _reader;

    @Override
    public void init(Map<String, String> props, Schema indexingSchema, String topicName) throws Exception {
      // Load Avro schema
      DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile);
      _avroSchema = reader.getSchema();
      reader.close();
      _rowGenerator = new AvroRecordToPinotRowGenerator(indexingSchema);
      _reader = new GenericDatumReader<>(_avroSchema);
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return decode(payload, 0, payload.length, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      try {
        GenericData.Record avroRecord =
            _reader.read(null, _decoderFactory.binaryDecoder(payload, offset, length, null));
        return _rowGenerator.transform(avroRecord, destination);
      } catch (Exception e) {
        LOGGER.error("Caught exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  protected void addRealtimeTable(String tableName, boolean useLlc, String kafkaBrokerList, String kafkaZkUrl,
      String kafkaTopic, int realtimeSegmentFlushRows, File avroFile, String timeColumnName, String timeType,
      String schemaName, String brokerTenant, String serverTenant, String loadMode, String sortedColumn,
      List<String> invertedIndexColumns, List<String> bloomFilterColumns, List<String> noDictionaryColumns,
      TableTaskConfig taskConfig, String streamConsumerFactoryName) throws Exception {
    Map<String, String> streamConfigs = new HashMap<>();
    String streamType = "kafka";
    streamConfigs.put(StreamConfigProperties.STREAM_TYPE, streamType);
    if (useLlc) {
      // LLC
      streamConfigs.put(
          StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
          StreamConfig.ConsumerType.LOWLEVEL.toString());
      streamConfigs.put(KafkaStreamConfigProperties.constructStreamProperty(
          KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST), kafkaBrokerList);
    } else {
      // HLC
      streamConfigs.put(
          StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
          StreamConfig.ConsumerType.HIGHLEVEL.toString());
      streamConfigs.put(KafkaStreamConfigProperties.constructStreamProperty(
          KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_ZK_CONNECTION_STRING), kafkaZkUrl);
    }
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), streamConsumerFactoryName);
    streamConfigs.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
        kafkaTopic);
    AvroFileSchemaKafkaAvroMessageDecoder.avroFile = avroFile;
    streamConfigs.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(realtimeSegmentFlushRows));
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");

    TableConfig tableConfig = new TableConfig.Builder(Helix.TableType.REALTIME).setTableName(tableName)
        .setLLC(useLlc)
        .setTimeColumnName(timeColumnName)
        .setTimeType(timeType)
        .setSchemaName(schemaName)
        .setBrokerTenant(brokerTenant)
        .setServerTenant(serverTenant)
        .setLoadMode(loadMode)
        .setSortedColumn(sortedColumn)
        .setInvertedIndexColumns(invertedIndexColumns)
        .setBloomFilterColumns(bloomFilterColumns)
        .setNoDictionaryColumns(noDictionaryColumns)
        .setStreamConfigs(streamConfigs)
        .setTaskConfig(taskConfig)
        .build();

    // save the realtime table config
    _realtimeTableConfig = tableConfig;

    if (!isUsingNewConfigFormat()) {
      sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJSONConfigString());
    }
  }

  protected void updateRealtimeTableConfig(String tablename, List<String> invertedIndexCols, List<String> bloomFilterCols) throws Exception {

    IndexingConfig config  = _realtimeTableConfig.getIndexingConfig();
    config.setInvertedIndexColumns(invertedIndexCols);
    config.setBloomFilterColumns(bloomFilterCols);

    sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tablename), _realtimeTableConfig.toJSONConfigString());
  }

  protected void dropRealtimeTable(String tableName) throws Exception {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType(tableName)));
  }

  protected void addHybridTable(String tableName, boolean useLlc, String kafkaBrokerList, String kafkaZkUrl,
      String kafkaTopic, int realtimeSegmentFlushSize, File avroFile, String timeColumnName, String timeType,
      String schemaName, String brokerTenant, String serverTenant, String loadMode, String sortedColumn,
      List<String> invertedIndexColumns, List<String> bloomFilterColumns, List<String> noDictionaryColumns,
      TableTaskConfig taskConfig, String streamConsumerFactoryName) throws Exception {
    addOfflineTable(tableName, timeColumnName, timeType, brokerTenant, serverTenant, loadMode, SegmentVersion.v1,
        invertedIndexColumns, bloomFilterColumns, taskConfig);
    addRealtimeTable(tableName, useLlc, kafkaBrokerList, kafkaZkUrl, kafkaTopic, realtimeSegmentFlushSize, avroFile,
        timeColumnName, timeType, schemaName, brokerTenant, serverTenant, loadMode, sortedColumn, invertedIndexColumns,
        bloomFilterColumns, noDictionaryColumns, taskConfig, streamConsumerFactoryName);
  }

  protected void createBrokerTenant(String tenantName, int brokerCount) throws Exception {
    String request = ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON(tenantName, brokerCount);
    sendPostRequest(_controllerRequestURLBuilder.forBrokerTenantCreate(), request);
  }

  protected void createServerTenant(String tenantName, int offlineServerCount, int realtimeServerCount)
      throws Exception {
    String request = ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON(tenantName,
        offlineServerCount + realtimeServerCount, offlineServerCount, realtimeServerCount);
    sendPostRequest(_controllerRequestURLBuilder.forServerTenantCreate(), request);
  }

  protected JsonNode getDebugInfo(final String uri) throws Exception {
    return JsonUtils.stringToJsonNode(sendGetRequest(_brokerBaseApiUrl + "/" + uri));
  }

  protected JsonNode postQuery(String query) throws Exception {
    return postQuery(query, _brokerBaseApiUrl);
  }

  public static JsonNode postQuery(String query, String brokerBaseApiUrl) throws Exception {
    return postQuery(query, brokerBaseApiUrl, false);
  }

  public static JsonNode postQuery(String query, String brokerBaseApiUrl, boolean enableTrace) throws Exception {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put("pql", query);
    payload.put("trace", enableTrace);
    payload.put("validateQuery", true);

    return JsonUtils.stringToJsonNode(sendPostRequest(brokerBaseApiUrl + "/query", payload.toString()));
  }
}
