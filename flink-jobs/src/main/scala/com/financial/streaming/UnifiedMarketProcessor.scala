package com.finance.streaming

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import com.finance.streaming.models._
import com.finance.streaming.processors._
import ujson._

object UnifiedMarketProcessor {
  
  val KAFKA_BROKERS = sys.env.getOrElse("KAFKA_BROKERS", "kafka:9092")
  val ICEBERG_CATALOG_URI = sys.env.getOrElse("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
  val S3_ENDPOINT = sys.env.getOrElse("S3_ENDPOINT", "http://minio:9000")
  
  val TOPICS = List(
    "stocks.AAPL", "stocks.GOOGL", "stocks.MSFT",
    "stocks.TSLA", "stocks.AMZN", "forex.EURUSD",
    "forex.GBPUSD", "crypto.BTCUSD"
  )

  def main(args: Array[String]): Unit = {
    println("=" * 50)
    println("🚀 Unified Market Data Processor - Starting")
    println("=" * 50)

    // Environment setup
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    
    env.setParallelism(4)
    env.enableCheckpointing(60000)

    // Configure Iceberg catalog
    configureIcebergCatalog(tableEnv)
    createIcebergTables(tableEnv)

    // Kafka source (new API)
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(KAFKA_BROKERS)
      .setTopics(TOPICS: _*)
      .setGroupId("unified-processor")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val rawStream = env.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(),
      "Kafka Source"
    )

    // Parse to MarketTick
    val marketStream: DataStream[MarketTick] = rawStream
      .map(parseMarketData)
      .filter(_ != null)
      .name("Parse Market Data")

    // === RAW DATA → ICEBERG ===
    writeRawDataToIceberg(tableEnv, marketStream)

    // === 1-MINUTE AGGREGATIONS ===
    val aggregatedStream = marketStream
      .keyBy(_.symbol)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      .aggregate(new MarketAggregator())
      .name("1-min Aggregation")

    writeAggregationsToIceberg(tableEnv, aggregatedStream)
    aggregatedStream.map(formatAggregate).print()

    // === VOLATILITY CALCULATION ===
    val volatilityStream = VolatilityProcessor.calculate(marketStream)
    writeVolatilityToIceberg(tableEnv, volatilityStream)
    volatilityStream.filter(_.historicalVolatility > 0).map(formatVolatility).print()

    // === ANOMALY DETECTION ===
    val anomalyStream = AnomalyProcessor.detect(marketStream)
    writeAnomaliesToIceberg(tableEnv, anomalyStream)
    anomalyStream.map(formatAnomaly).print()

    // === TECHNICAL INDICATORS ===
    val indicatorsStream = TechnicalIndicatorProcessor.calculate(marketStream)
    writeIndicatorsToIceberg(tableEnv, indicatorsStream)

    println("🎬 Starting unified pipeline...")
    env.execute("Unified Market Data Processor")
  }

  def parseMarketData(json: String): MarketTick = {
    try {
      val d = ujson.read(json)
      MarketTick(
        symbol = d("symbol").str,
        timestamp = System.currentTimeMillis(),
        price = d("price").num,
        bid = d.obj.get("bid").map(_.num).getOrElse(d("price").num - 0.01),
        ask = d.obj.get("ask").map(_.num).getOrElse(d("price").num + 0.01),
        volume = d("volume").num.toInt,
        exchange = d("exchange").str,
        messageType = d.obj.get("message_type").map(_.str).getOrElse("trade")
      )
    } catch {
      case e: Exception =>
        System.err.println(s"Parse error: ${e.getMessage}")
        null
    }
  }

  def configureIcebergCatalog(tableEnv: StreamTableEnvironment): Unit = {
    tableEnv.executeSql(s"""
      CREATE CATALOG iceberg WITH (
        'type' = 'iceberg',
        'catalog-type' = 'rest',
        'uri' = '$ICEBERG_CATALOG_URI',
        'warehouse' = 's3://warehouse/',
        'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint' = '$S3_ENDPOINT',
        's3.path-style-access' = 'true'
      )
    """)
    tableEnv.executeSql("USE CATALOG iceberg")
    tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS financial_db")
    tableEnv.executeSql("USE financial_db")
    println("✅ Iceberg catalog configured")
  }

  def createIcebergTables(tableEnv: StreamTableEnvironment): Unit = {
    // Raw ticks table
    tableEnv.executeSql("""
      CREATE TABLE IF NOT EXISTS market_ticks (
        symbol STRING,
        event_time TIMESTAMP(3),
        price DOUBLE,
        bid DOUBLE,
        ask DOUBLE,
        volume INT,
        exchange STRING,
        message_type STRING,
        dt STRING
      ) PARTITIONED BY (dt, symbol)
    """)

    // Aggregations table
    tableEnv.executeSql("""
      CREATE TABLE IF NOT EXISTS market_aggregates_1min (
        symbol STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        close_price DOUBLE,
        avg_price DOUBLE,
        total_volume BIGINT,
        tick_count INT,
        volatility DOUBLE,
        dt STRING
      ) PARTITIONED BY (dt, symbol)
    """)

    // Volatility table
    tableEnv.executeSql("""
      CREATE TABLE IF NOT EXISTS volatility_metrics (
        symbol STRING,
        calc_time TIMESTAMP(3),
        price DOUBLE,
        historical_vol DOUBLE,
        realized_vol DOUBLE,
        sharpe_ratio DOUBLE,
        var_95 DOUBLE,
        max_drawdown DOUBLE,
        dt STRING
      ) PARTITIONED BY (dt, symbol)
    """)

    // Anomalies table
    tableEnv.executeSql("""
      CREATE TABLE IF NOT EXISTS market_anomalies (
        symbol STRING,
        event_time TIMESTAMP(3),
        anomaly_type STRING,
        severity STRING,
        price DOUBLE,
        expected_price DOUBLE,
        deviation_pct DOUBLE,
        z_score DOUBLE,
        confidence DOUBLE,
        description STRING,
        dt STRING
      ) PARTITIONED BY (dt, symbol)
    """)

    // Technical indicators table
    tableEnv.executeSql("""
      CREATE TABLE IF NOT EXISTS technical_indicators (
        symbol STRING,
        calc_time TIMESTAMP(3),
        price DOUBLE,
        sma_20 DOUBLE,
        sma_50 DOUBLE,
        ema_12 DOUBLE,
        ema_26 DOUBLE,
        rsi_14 DOUBLE,
        macd DOUBLE,
        macd_signal DOUBLE,
        bollinger_upper DOUBLE,
        bollinger_middle DOUBLE,
        bollinger_lower DOUBLE,
        dt STRING
      ) PARTITIONED BY (dt, symbol)
    """)

    println("✅ Iceberg tables created")
  }

  def writeRawDataToIceberg(tableEnv: StreamTableEnvironment, stream: DataStream[MarketTick]): Unit = {
    val table = tableEnv.fromDataStream(stream)
    tableEnv.createTemporaryView("raw_ticks", table)
    tableEnv.executeSql("""
      INSERT INTO market_ticks
      SELECT symbol, TO_TIMESTAMP_LTZ(timestamp, 3), price, bid, ask, 
             volume, exchange, messageType, DATE_FORMAT(NOW(), 'yyyy-MM-dd')
      FROM raw_ticks
    """)
  }

  def writeAggregationsToIceberg(tableEnv: StreamTableEnvironment, stream: DataStream[MarketAggregate]): Unit = {
    val table = tableEnv.fromDataStream(stream)
    tableEnv.createTemporaryView("agg_stream", table)
    tableEnv.executeSql("""
      INSERT INTO market_aggregates_1min
      SELECT symbol, TO_TIMESTAMP_LTZ(windowStart, 3), TO_TIMESTAMP_LTZ(windowEnd, 3),
             openPrice, highPrice, lowPrice, closePrice, avgPrice, totalVolume,
             tickCount, volatility, DATE_FORMAT(NOW(), 'yyyy-MM-dd')
      FROM agg_stream
    """)
  }

  def writeVolatilityToIceberg(tableEnv: StreamTableEnvironment, stream: DataStream[VolatilityMetrics]): Unit = {
    val table = tableEnv.fromDataStream(stream)
    tableEnv.createTemporaryView("vol_stream", table)
    tableEnv.executeSql("""
      INSERT INTO volatility_metrics
      SELECT symbol, TO_TIMESTAMP_LTZ(timestamp, 3), price, historicalVolatility,
             realizedVolatility, sharpeRatio, valueAtRisk95, maxDrawdown,
             DATE_FORMAT(NOW(), 'yyyy-MM-dd')
      FROM vol_stream
    """)
  }

  def writeAnomaliesToIceberg(tableEnv: StreamTableEnvironment, stream: DataStream[MarketAnomaly]): Unit = {
    val table = tableEnv.fromDataStream(stream)
    tableEnv.createTemporaryView("anomaly_stream", table)
    tableEnv.executeSql("""
      INSERT INTO market_anomalies
      SELECT symbol, TO_TIMESTAMP_LTZ(timestamp, 3), anomalyType, severity,
             currentPrice, expectedPrice, deviationPct, zScore, confidence,
             description, DATE_FORMAT(NOW(), 'yyyy-MM-dd')
      FROM anomaly_stream
    """)
  }

  def writeIndicatorsToIceberg(tableEnv: StreamTableEnvironment, stream: DataStream[TechnicalIndicators]): Unit = {
    val table = tableEnv.fromDataStream(stream)
    tableEnv.createTemporaryView("tech_stream", table)
    tableEnv.executeSql("""
      INSERT INTO technical_indicators
      SELECT symbol, TO_TIMESTAMP_LTZ(timestamp, 3), price, sma20, sma50,
             ema12, ema26, rsi14, macd, macdSignal, bollingerUpper,
             bollingerMiddle, bollingerLower, DATE_FORMAT(NOW(), 'yyyy-MM-dd')
      FROM tech_stream
    """)
  }

  // Formatting helpers
  def formatAggregate(a: MarketAggregate): String =
    s"[AGG] ${a.symbol}: O=${f(a.openPrice)} H=${f(a.highPrice)} L=${f(a.lowPrice)} C=${f(a.closePrice)} Vol=${a.totalVolume}"

  def formatVolatility(v: VolatilityMetrics): String =
    s"[VOL] ${v.symbol}: HV=${pct(v.historicalVolatility)} Sharpe=${f(v.sharpeRatio)} VaR95=${pct(v.valueAtRisk95)}"

  def formatAnomaly(a: MarketAnomaly): String =
    s"[ANOMALY] ${a.symbol} ${a.anomalyType} [${a.severity}]: ${a.description}"

  private def f(d: Double): String = f"$d%.2f"
  private def pct(d: Double): String = f"${d * 100}%.2f%%"
}