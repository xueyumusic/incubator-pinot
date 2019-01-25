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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.TableQueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.broker.routing.TimeBoundaryService;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Broker.*;


@ThreadSafe
public abstract class BaseBrokerRequestHandler implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrokerRequestHandler.class);
  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();

  protected final Configuration _config;
  protected final RoutingTable _routingTable;
  protected final TimeBoundaryService _timeBoundaryService;
  protected final AccessControlFactory _accessControlFactory;
  protected final TableQueryQuotaManager _tableQueryQuotaManager;
  protected final TableSchemaCache _tableSchemaCache;
  protected final BrokerMetrics _brokerMetrics;

  protected final AtomicLong _requestIdGenerator = new AtomicLong();
  protected final BrokerRequestOptimizer _brokerRequestOptimizer = new BrokerRequestOptimizer();
  protected final BrokerReduceService _brokerReduceService = new BrokerReduceService();

  protected final String _brokerId;
  protected final long _brokerTimeoutMs;
  protected final int _queryResponseLimit;
  protected final int _queryLogLength;

  public BaseBrokerRequestHandler(Configuration config, RoutingTable routingTable,
      TimeBoundaryService timeBoundaryService, AccessControlFactory accessControlFactory,
      TableQueryQuotaManager tableQueryQuotaManager, TableSchemaCache tableSchemaCache,
      BrokerMetrics brokerMetrics) {
    _config = config;
    _routingTable = routingTable;
    _timeBoundaryService = timeBoundaryService;
    _accessControlFactory = accessControlFactory;
    _tableQueryQuotaManager = tableQueryQuotaManager;
    _tableSchemaCache = tableSchemaCache;
    _brokerMetrics = brokerMetrics;

    _brokerId = config.getString(CONFIG_OF_BROKER_ID, getDefaultBrokerId());
    _brokerTimeoutMs = config.getLong(CONFIG_OF_BROKER_TIMEOUT_MS, DEFAULT_BROKER_TIMEOUT_MS);
    _queryResponseLimit = config.getInt(CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT, DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
    _queryLogLength = config.getInt(CONFIG_OF_BROKER_QUERY_LOG_LENGTH, DEFAULT_BROKER_QUERY_LOG_LENGTH);

    LOGGER.info("Broker Id: {}, timeout: {}ms, query response limit: {}, query log length: {}", _brokerId,
        _brokerTimeoutMs, _queryResponseLimit, _queryLogLength);
  }

  private String getDefaultBrokerId() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting default broker Id", e);
      return "";
    }
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestStatistics requestStatistics)
      throws Exception {
    long requestId = _requestIdGenerator.incrementAndGet();
    requestStatistics.setBrokerId(_brokerId);
    requestStatistics.setRequestId(requestId);
    requestStatistics.setRequestArrivalTimeMillis(System.currentTimeMillis());

    RequestParams requestParams = new RequestParams(request);
    String query = requestParams.getQuery();
    LOGGER.debug("Query string for request {}: {}", requestId, query);
    requestStatistics.setPql(query);

    // Compile the request
    long compilationStartTimeNs = System.nanoTime();
    BrokerRequest brokerRequest;
    try {
      brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
    } catch (Exception e) {
      LOGGER.info("Caught exception while compiling request {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      requestStatistics.setErrorCode(QueryException.PQL_PARSING_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.PQL_PARSING_ERROR, e));
    }
    String tableName = brokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    requestStatistics.setTableName(rawTableName);
    long compilationEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REQUEST_COMPILATION,
        compilationEndTimeNs - compilationStartTimeNs);
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERIES, 1);

    // Check table access
    boolean hasAccess = _accessControlFactory.create().hasAccess(requesterIdentity, brokerRequest);
    if (!hasAccess) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.info("Access denied for requestId {}, table {}", requestId, tableName);
      requestStatistics.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      return new BrokerResponseNative(QueryException.ACCESS_DENIED_ERROR);
    }
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.AUTHORIZATION,
        System.nanoTime() - compilationEndTimeNs);

    // Get the tables hit by the request
    String offlineTableName = null;
    String realtimeTableName = null;
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      // Offline table
      if (_routingTable.routingTableExists(tableName)) {
        offlineTableName = tableName;
      }
    } else if (tableType == CommonConstants.Helix.TableType.REALTIME) {
      // Realtime table
      if (_routingTable.routingTableExists(tableName)) {
        realtimeTableName = tableName;
      }
    } else {
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      if (_routingTable.routingTableExists(offlineTableNameToCheck)) {
        offlineTableName = offlineTableNameToCheck;
      }
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      if (_routingTable.routingTableExists(realtimeTableNameToCheck)) {
        realtimeTableName = realtimeTableNameToCheck;
      }
    }
    if ((offlineTableName == null) && (realtimeTableName == null)) {
      // No table matches the request
      LOGGER.info("No table matches for request {}: {}", requestId, query);
      requestStatistics.setErrorCode(QueryException.BROKER_RESOURCE_MISSING_ERROR_CODE);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.RESOURCE_MISSING_EXCEPTIONS, 1);
      return BrokerResponseNative.NO_TABLE_RESULT;
    }

    // Validate QPS quota
    if (!_tableQueryQuotaManager.acquire(tableName)) {
      String errorMessage =
          String.format("Request %d exceeds query quota for table:%s, query:%s", requestId, tableName, query);
      LOGGER.info(errorMessage);
      requestStatistics.setErrorCode(QueryException.TOO_MANY_REQUESTS_ERROR_CODE);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_QUOTA_EXCEEDED, 1);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
    }

    // Validate the request
    try {
      validateRequest(brokerRequest, requestParams);
    } catch (Exception e) {
      LOGGER.info("Caught exception while validating request {}: {}, {}", requestId, query, e.getMessage());
      requestStatistics.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, e));
    }

    // Set extra settings into broker request
    if (requestParams.isTrace()) {
      LOGGER.debug("Enable trace for request {}: {}", requestId, query);
      brokerRequest.setEnableTrace(true);
    }
    Map<String, String> debugOptions = requestParams.getDebugOptions();
    if (debugOptions != null) {
      LOGGER.debug("Debug options are set to: {} for request {}: {}", debugOptions, requestId, query);
      brokerRequest.setDebugOptions(debugOptions);
    }

    // Optimize the query
    // TODO: get time column name from schema or table config so that we can apply it for REALTIME only case
    // We get timeColumnName from time boundary service currently, which only exists for offline table
    String timeColumn = getTimeColumnName(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    if ((offlineTableName != null) && (realtimeTableName != null)) {
      // Hybrid
      offlineBrokerRequest = _brokerRequestOptimizer.optimize(getOfflineBrokerRequest(brokerRequest), timeColumn);
      realtimeBrokerRequest = _brokerRequestOptimizer.optimize(getRealtimeBrokerRequest(brokerRequest), timeColumn);
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.HYBRID);
    } else if (offlineTableName != null) {
      // OFFLINE only
      brokerRequest.getQuerySource().setTableName(offlineTableName);
      offlineBrokerRequest = _brokerRequestOptimizer.optimize(brokerRequest, timeColumn);
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.OFFLINE);
    } else {
      // REALTIME only
      brokerRequest.getQuerySource().setTableName(realtimeTableName);
      realtimeBrokerRequest = _brokerRequestOptimizer.optimize(brokerRequest, timeColumn);
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.REALTIME);
    }

    // Calculate routing table for the query
    long routingStartTimeNs = System.nanoTime();
    Map<String, List<String>> offlineRoutingTable = null;
    Map<String, List<String>> realtimeRoutingTable = null;
    if (offlineBrokerRequest != null) {
      offlineRoutingTable = _routingTable.getRoutingTable(new RoutingTableLookupRequest(offlineBrokerRequest));
      if (offlineRoutingTable.isEmpty()) {
        LOGGER.debug("No OFFLINE server found for request {}: {}", requestId, query);
        offlineBrokerRequest = null;
        offlineRoutingTable = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      realtimeRoutingTable = _routingTable.getRoutingTable(new RoutingTableLookupRequest(realtimeBrokerRequest));
      if (realtimeRoutingTable.isEmpty()) {
        LOGGER.debug("No REALTIME server found for request {}: {}", requestId, query);
        realtimeBrokerRequest = null;
        realtimeRoutingTable = null;
      }
    }
    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      LOGGER.info("No server found for request {}: {}", requestId, query);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
      return BrokerResponseNative.EMPTY_RESULT;
    }
    long routingEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_ROUTING, routingEndTimeNs - routingStartTimeNs);

    // Execute the query
    long remainingTimeMs = _brokerTimeoutMs - TimeUnit.NANOSECONDS.toMillis(routingEndTimeNs - compilationStartTimeNs);
    ServerStats serverStats = new ServerStats();
    BrokerResponse brokerResponse =
        processBrokerRequest(requestId, brokerRequest, offlineBrokerRequest, offlineRoutingTable, realtimeBrokerRequest,
            realtimeRoutingTable, remainingTimeMs, serverStats, requestStatistics);
    long executionEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_EXECUTION,
        executionEndTimeNs - routingEndTimeNs);

    // Track number of queries with number of groups limit reached
    if (brokerResponse.isNumGroupsLimitReached()) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED, 1);
    }

    // Set total query processing time
    long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(executionEndTimeNs - compilationStartTimeNs);
    brokerResponse.setTimeUsedMs(totalTimeMs);
    requestStatistics.setQueryProcessingTime(totalTimeMs);
    requestStatistics.setStatistics(brokerResponse);

    LOGGER.debug("Broker Response: {}", brokerResponse);

    // Table name might have been changed (with suffix _OFFLINE/_REALTIME appended)
    LOGGER.info(
        "RequestId:{}, table:{}, timeMs:{}, docs:{}/{}, entries:{}/{}, segments(queried/processed/matched):{}/{}/{} "
            + "servers:{}/{}, groupLimitReached:{}, exceptions:{}, serverStats:{}, query:{}", requestId,
        brokerRequest.getQuerySource().getTableName(), totalTimeMs, brokerResponse.getNumDocsScanned(),
        brokerResponse.getTotalDocs(), brokerResponse.getNumEntriesScannedInFilter(),
        brokerResponse.getNumEntriesScannedPostFilter(), brokerResponse.getNumSegmentsQueried(),
        brokerResponse.getNumSegmentsProcessed(), brokerResponse.getNumSegmentsMatched(),
        brokerResponse.getNumServersResponded(), brokerResponse.getNumServersQueried(),
        brokerResponse.isNumGroupsLimitReached(), brokerResponse.getExceptionsSize(), serverStats.getServerStats(),
        StringUtils.substring(query, 0, _queryLogLength));

    return brokerResponse;
  }

  /**
   * Broker side validation on the broker request.
   * <p>Throw RuntimeException if query does not pass validation.
   * <p>Current validations are:
   * <ul>
   *   <li>Value for 'TOP' for aggregation group-by query <= configured value</li>
   *   <li>Value for 'LIMIT' for selection query <= configured value</li>
   * </ul>
   */
  private void validateRequest(BrokerRequest brokerRequest, RequestParams requestParams) {
    if (brokerRequest.isSetAggregationsInfo()) {
      if (brokerRequest.isSetGroupBy()) {
        long topN = brokerRequest.getGroupBy().getTopN();
        if (topN > _queryResponseLimit) {
          throw new RuntimeException(
              "Value for 'TOP' (" + topN + ") exceeds maximum allowed value of " + _queryResponseLimit);
        }
      }
    } else {
      int limit = brokerRequest.getSelections().getSize();
      if (limit > _queryResponseLimit) {
        throw new RuntimeException(
            "Value for 'LIMIT' (" + limit + ") exceeds maximum allowed value of " + _queryResponseLimit);
      }
    }

    // Checks whether the query contains non-existent columns.
    // Table name has already been verified before hitting this line.
    if (requestParams.isValidateQuery()) {
      String tableName = brokerRequest.getQuerySource().getTableName();
      Schema schema = _tableSchemaCache.getIfTableSchemaPresent(tableName);
      if (schema != null) {
        Set<String> columnsFromBrokerRequest = getAllColumnsFromBrokerRequest(brokerRequest);
        // Filters out virtual columns in the query.
        columnsFromBrokerRequest.removeIf(column -> column.startsWith("$"));
        columnsFromBrokerRequest.removeAll(schema.getColumnNames());
        if (!columnsFromBrokerRequest.isEmpty()) {
          _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.QUERY_NON_EXISTENT_COLUMNS, 1L);
          throw new RuntimeException(
              "Found non-existent columns from the query: " + columnsFromBrokerRequest.toString());
        }
      } else {
        // If the cache doesn't have the schema, loads the schema to the cache asynchronously.
        _tableSchemaCache.refreshTableSchema(tableName);
      }
    }
  }

  /**
   * Helper to get all the columns from broker request.
   * Returns the set of all the columns.
   */
  private Set<String> getAllColumnsFromBrokerRequest(BrokerRequest brokerRequest) {
    Set<String> allColumns = new HashSet<>();
    // Filter
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    if (filterQueryTree != null) {
      allColumns.addAll(RequestUtils.extractFilterColumns(filterQueryTree));
    }

    // Aggregation
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo != null) {
      Set<TransformExpressionTree> _aggregationExpressions = new HashSet<>();
      for (AggregationInfo aggregationInfo : aggregationsInfo) {
        if (!aggregationInfo.getAggregationType().equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
          _aggregationExpressions.add(
              TransformExpressionTree.compileToExpressionTree(AggregationFunctionUtils.getColumn(aggregationInfo)));
        }
      }
      allColumns.addAll(RequestUtils.extractColumnsFromExpressions(_aggregationExpressions));
    }

    // Group-by
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      Set<TransformExpressionTree> groupByExpressions = new HashSet<>();
      for (String expression : groupBy.getExpressions()) {
        groupByExpressions.add(TransformExpressionTree.compileToExpressionTree(expression));
      }
      allColumns.addAll(RequestUtils.extractColumnsFromExpressions(groupByExpressions));
    }

    // Selection
    Selection selection = brokerRequest.getSelections();
    if (selection != null) {
      allColumns.addAll(RequestUtils.extractSelectionColumns(selection));
    }

    return allColumns;
  }

  /**
   * Helper method to get the time column name for the OFFLINE table name from the time boundary service, or
   * <code>null</code> if the time boundary service does not have the information.
   */
  private String getTimeColumnName(String offlineTableName) {
    TimeBoundaryService.TimeBoundaryInfo timeBoundaryInfo =
        _timeBoundaryService.getTimeBoundaryInfoFor(offlineTableName);
    return (timeBoundaryInfo != null) ? timeBoundaryInfo.getTimeColumn() : null;
  }

  /**
   * Helper method to create an OFFLINE broker request from the given hybrid broker request.
   * <p>This step will attach the time boundary to the request.
   */
  private BrokerRequest getOfflineBrokerRequest(BrokerRequest hybridBrokerRequest) {
    BrokerRequest offlineRequest = hybridBrokerRequest.deepCopy();
    String rawTableName = hybridBrokerRequest.getQuerySource().getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    offlineRequest.getQuerySource().setTableName(offlineTableName);
    attachTimeBoundary(rawTableName, offlineRequest, true);
    return offlineRequest;
  }

  /**
   * Helper method to create a REALTIME broker request from the given hybrid broker request.
   * <p>This step will attach the time boundary to the request.
   */
  private BrokerRequest getRealtimeBrokerRequest(BrokerRequest hybridBrokerRequest) {
    BrokerRequest realtimeRequest = hybridBrokerRequest.deepCopy();
    String rawTableName = hybridBrokerRequest.getQuerySource().getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    realtimeRequest.getQuerySource().setTableName(realtimeTableName);
    attachTimeBoundary(rawTableName, realtimeRequest, false);
    return realtimeRequest;
  }

  /**
   * Helper method to attach time boundary to a broker request.
   */
  private void attachTimeBoundary(String rawTableName, BrokerRequest brokerRequest, boolean isOfflineRequest) {
    TimeBoundaryService.TimeBoundaryInfo timeBoundaryInfo =
        _timeBoundaryService.getTimeBoundaryInfoFor(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    if (timeBoundaryInfo == null || timeBoundaryInfo.getTimeColumn() == null
        || timeBoundaryInfo.getTimeValue() == null) {
      LOGGER.warn("Failed to find time boundary info for hybrid table: {}", rawTableName);
      return;
    }

    // Create a time range filter
    FilterQuery timeFilterQuery = new FilterQuery();
    // Use -1 to prevent collision with other filters
    timeFilterQuery.setId(-1);
    timeFilterQuery.setColumn(timeBoundaryInfo.getTimeColumn());
    String timeValue = timeBoundaryInfo.getTimeValue();
    String filterValue = isOfflineRequest ? "(*\t\t" + timeValue + ")" : "[" + timeValue + "\t\t*)";
    timeFilterQuery.setValue(Collections.singletonList(filterValue));
    timeFilterQuery.setOperator(FilterOperator.RANGE);
    timeFilterQuery.setNestedFilterQueryIds(Collections.emptyList());

    // Attach the time range filter to the current filters
    FilterQuery currentFilterQuery = brokerRequest.getFilterQuery();
    if (currentFilterQuery != null) {
      FilterQuery andFilterQuery = new FilterQuery();
      // Use -2 to prevent collision with other filters
      andFilterQuery.setId(-2);
      andFilterQuery.setOperator(FilterOperator.AND);
      List<Integer> nestedFilterQueryIds = new ArrayList<>(2);
      nestedFilterQueryIds.add(currentFilterQuery.getId());
      nestedFilterQueryIds.add(timeFilterQuery.getId());
      brokerRequest.setFilterQuery(andFilterQuery);

      andFilterQuery.setNestedFilterQueryIds(nestedFilterQueryIds);
      FilterQueryMap filterSubQueryMap = brokerRequest.getFilterSubQueryMap();
      filterSubQueryMap.putToFilterQueryMap(-1, timeFilterQuery);
      filterSubQueryMap.putToFilterQueryMap(-2, andFilterQuery);
      brokerRequest.setFilterSubQueryMap(filterSubQueryMap);
    } else {
      FilterQueryMap filterSubQueryMap = new FilterQueryMap();
      filterSubQueryMap.putToFilterQueryMap(-1, timeFilterQuery);
      brokerRequest.setFilterQuery(timeFilterQuery);
      brokerRequest.setFilterSubQueryMap(filterSubQueryMap);
    }
  }

  /**
   * Processes the optimized broker requests for both OFFLINE and REALTIME table.
   */
  protected abstract BrokerResponse processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<String, List<String>> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<String, List<String>> realtimeRoutingTable,
      long timeoutMs, ServerStats serverStats, RequestStatistics requestStatistics) throws Exception;

  /**
   * Helper class to pass the per server statistics.
   */
  protected static class ServerStats {
    private String _serverStats;

    public void setServerStats(String serverStats) {
      _serverStats = serverStats;
    }

    public String getServerStats() {
      return _serverStats;
    }
  }
}
