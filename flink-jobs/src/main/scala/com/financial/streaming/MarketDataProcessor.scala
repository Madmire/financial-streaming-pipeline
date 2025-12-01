package com.finance.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import java.util.Properties
import ujson._

case class MarketTick(
  symbol: String,
  timestamp: Long,
  price: Double,
  bid: Double,
  ask: Double,
  volume: Int,
  exchange: String,
  message_type: String
)

case class ProcessedMarketData(
  symbol: String,
  window_start: Long,
  window_end: Long,
  avg_price: Double,
  min_price: Double,
  max_price: Double,
  total_volume: Long,
  tick_count: Int,
  volatility: Double,
  spread: Double
)

object MarketDataIcebergProcessor {

  def main(args: Array[String]): Unit = {
    // Environnements Flink
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setParallelism(4)
    
    // Checkpoint pour garantir exactly-once
    env.enableCheckpointing(60000) // Checkpoint toutes les 60s

    // Configuration Kafka
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "kafka:9092")
    kafkaProps.setProperty("group.id", "flink-iceberg-processor")

    val topics = List(
      "stocks.AAPL", "stocks.GOOGL", "stocks.MSFT",
      "stocks.TSLA", "stocks.AMZN", "forex.EURUSD",
      "forex.GBPUSD", "crypto.BTCUSD"
    )

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      java.util.Arrays.asList(topics: _*),
      new SimpleStringSchema(),
      kafkaProps
    )
    kafkaConsumer.setStartFromLatest()

    // Stream de données brutes
    val rawStream = env.addSource(kafkaConsumer)

    val marketStream: DataStream[MarketTick] = rawStream
      .map(parseMarketData)
      .filter(_ != null)

    // === 1. Écriture des données brutes dans Iceberg ===
    
    // Convertir en Table pour Iceberg
    val rawTable = tableEnv.fromDataStream(marketStream)
    tableEnv.createTemporaryView("raw_market_data", rawTable)

    // Créer la table Iceberg pour les données brutes
    tableEnv.executeSql("""
      CREATE TABLE IF NOT EXISTS iceberg_catalog.financial_db.market_ticks (
        symbol STRING,
        timestamp BIGINT,
        price DOUBLE,
        bid DOUBLE,
        ask DOUBLE,
        volume INT,
        exchange STRING,
        message_type STRING,
        ingestion_time TIMESTAMP(3) METADATA FROM 'timestamp'
      ) WITH (
        'connector' = 'iceberg',
        'catalog-name' = 'iceberg_catalog',
        'catalog-type' = 'hive',
        'uri' = 'thrift://hive-metastore:9083',
        'warehouse' = 's3://warehouse/',
        'format-version' = '2'
      )
    """)

    // Insérer les données brutes
    tableEnv.executeSql("""
      INSERT INTO iceberg_catalog.financial_db.market_ticks
      SELECT symbol, timestamp, price, bid, ask, volume, exchange, message_type, CURRENT_TIMESTAMP
      FROM raw_market_data
    """)

    // === 2. Agrégations par fenêtre ===
    
    val aggregatedStream = marketStream
      .keyBy(_.symbol)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      .aggregate(new MarketDataAggregator())

    // Convertir en Table
    val aggTable = tableEnv.fromDataStream(aggregatedStream)
    tableEnv.createTemporaryView("aggregated_market_data", aggTable)

    // Créer la table Iceberg pour les agrégations
    tableEnv.executeSql("""
      CREATE TABLE IF NOT EXISTS iceberg_catalog.financial_db.market_aggregates_1min (
        symbol STRING,
        window_start BIGINT,
        window_end BIGINT,
        avg_price DOUBLE,
        min_price DOUBLE,
        max_price DOUBLE,
        total_volume BIGINT,
        tick_count INT,
        volatility DOUBLE,
        spread DOUBLE,
        processing_time TIMESTAMP(3)
      ) PARTITIONED BY (symbol) WITH (
        'connector' = 'iceberg',
        'catalog-name' = 'iceberg_catalog',
        'catalog-type' = 'hive',
        'uri' = 'thrift://hive-metastore:9083',
        'warehouse' = 's3://warehouse/',
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
      )
    """)

    // Insérer les agrégations
    tableEnv.executeSql("""
      INSERT INTO iceberg_catalog.financial_db.market_aggregates_1min
      SELECT 
        symbol, window_start, window_end, avg_price, min_price, max_price,
        total_volume, tick_count, volatility, spread, CURRENT_TIMESTAMP
      FROM aggregated_market_data
    """)

    // Affichage console pour debug
    aggregatedStream.print()

    env.execute("Market Data Iceberg Processor")
  }

  def parseMarketData(json: String): MarketTick = {
    try {
      val data = ujson.read(json)
      MarketTick(
        symbol = data("symbol").str,
        timestamp = System.currentTimeMillis(),
        price = data("price").num,
        bid = data.obj.get("bid").map(_.num).getOrElse(data("price").num - 0.01),
        ask = data.obj.get("ask").map(_.num).getOrElse(data("price").num + 0.01),
        volume = data("volume").num.toInt,
        exchange = data("exchange").str,
        message_type = data.obj.get("message_type").map(_.str).getOrElse("trade")
      )
    } catch {
      case e: Exception =>
        println(s"Error parsing JSON: $json, error: ${e.getMessage}")
        null
    }
  }
}

// Aggregator personnalisé
import org.apache.flink.api.common.functions.AggregateFunction

class MarketDataAggregator extends AggregateFunction[MarketTick, MarketDataAccumulator, ProcessedMarketData] {
  
  override def createAccumulator(): MarketDataAccumulator = MarketDataAccumulator()
  
  override def add(tick: MarketTick, acc: MarketDataAccumulator): MarketDataAccumulator = {
    val newPrices = acc.prices :+ tick.price
    val newSpreads = acc.spreads :+ (tick.ask - tick.bid)
    
    MarketDataAccumulator(
      symbol = tick.symbol,
      windowStart = if (acc.windowStart == 0L) tick.timestamp else acc.windowStart,
      windowEnd = tick.timestamp,
      prices = newPrices,
      volumes = acc.volumes + tick.volume,
      tickCount = acc.tickCount + 1,
      spreads = newSpreads
    )
  }
  
  override def getResult(acc: MarketDataAccumulator): ProcessedMarketData = {
    val avgPrice = acc.prices.sum / acc.prices.length
    val minPrice = acc.prices.min
    val maxPrice = acc.prices.max
    val avgSpread = acc.spreads.sum / acc.spreads.length
    
    // Calcul de la volatilité (écart-type)
    val variance = acc.prices.map(p => Math.pow(p - avgPrice, 2)).sum / acc.prices.length
    val volatility = Math.sqrt(variance)
    
    ProcessedMarketData(
      symbol = acc.symbol,
      window_start = acc.windowStart,
      window_end = acc.windowEnd,
      avg_price = avgPrice,
      min_price = minPrice,
      max_price = maxPrice,
      total_volume = acc.volumes,
      tick_count = acc.tickCount,
      volatility = volatility,
      spread = avgSpread
    )
  }
  
  override def merge(a: MarketDataAccumulator, b: MarketDataAccumulator): MarketDataAccumulator = {
    MarketDataAccumulator(
      symbol = a.symbol,
      windowStart = Math.min(a.windowStart, b.windowStart),
      windowEnd = Math.max(a.windowEnd, b.windowEnd),
      prices = a.prices ++ b.prices,
      volumes = a.volumes + b.volumes,
      tickCount = a.tickCount + b.tickCount,
      spreads = a.spreads ++ b.spreads
    )
  }
}

case class MarketDataAccumulator(
  symbol: String = "",
  windowStart: Long = 0L,
  windowEnd: Long = 0L,
  prices: Seq[Double] = Seq.empty,
  volumes: Long = 0L,
  tickCount: Int = 0,
  spreads: Seq[Double] = Seq.empty
)