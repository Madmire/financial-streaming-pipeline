package com.finance.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer
import scala.math._

/**
 * Case classes pour les métriques de volatilité
 */
case class VolatilityMetrics(
  symbol: String,
  timestamp: Long,
  windowStart: Long,
  windowEnd: Long,
  // Prix
  price: Double,
  priceChange: Double,
  priceChangePct: Double,
  // Volatilité
  historicalVolatility: Double,      // Volatilité historique (écart-type)
  realizedVolatility: Double,        // Volatilité réalisée
  parkinsonVolatility: Double,       // Estimateur de Parkinson
  garmanKlassVolatility: Double,     // Estimateur de Garman-Klass
  // Statistiques
  returns: Seq[Double],
  avgReturn: Double,
  skewness: Double,                  // Asymétrie des rendements
  kurtosis: Double,                  // Kurtosis (queues de distribution)
  // Bandes de volatilité
  volatilityUpper: Double,
  volatilityLower: Double,
  // Indicateurs de risque
  sharpeRatio: Double,
  valueAtRisk95: Double,
  valueAtRisk99: Double,
  maxDrawdown: Double
)

case class PriceAccumulator(
  symbol: String,
  prices: ListBuffer[Double] = ListBuffer(),
  highPrices: ListBuffer[Double] = ListBuffer(),
  lowPrices: ListBuffer[Double] = ListBuffer(),
  openPrices: ListBuffer[Double] = ListBuffer(),
  closePrices: ListBuffer[Double] = ListBuffer(),
  timestamps: ListBuffer[Long] = ListBuffer(),
  volumes: ListBuffer[Long] = ListBuffer()
)

/**
 * Calculateur de volatilité avec multiples estimateurs
 */
object VolatilityCalculator {

  /**
   * Pipeline principal de calcul de volatilité
   */
  def calculateVolatility(
    marketStream: DataStream[MarketTick],
    windowSize: Time = Time.minutes(5),
    slideSize: Time = Time.minutes(1)
  ): DataStream[VolatilityMetrics] = {
    
    marketStream
      .keyBy(_.symbol)
      .window(SlidingProcessingTimeWindows.of(windowSize, slideSize))
      .aggregate(new VolatilityAggregator())
      .process(new VolatilityEnricher())
  }

  /**
   * Volatilité sur fenêtres multiples (multi-timeframe)
   */
  def calculateMultiTimeframeVolatility(
    marketStream: DataStream[MarketTick]
  ): DataStream[MultiTimeframeVolatility] = {
    
    val vol1min = calculateVolatility(marketStream, Time.minutes(1), Time.seconds(30))
    val vol5min = calculateVolatility(marketStream, Time.minutes(5), Time.minutes(1))
    val vol15min = calculateVolatility(marketStream, Time.minutes(15), Time.minutes(5))
    val vol1hour = calculateVolatility(marketStream, Time.hours(1), Time.minutes(15))
    
    // Combiner les différentes timeframes
    vol1min
      .keyBy(_.symbol)
      .connect(vol5min.keyBy(_.symbol))
      .flatMap(new MultiTimeframeCombiner())
  }

  /**
   * Aggregator pour collecter les données de prix
   */
  class VolatilityAggregator extends AggregateFunction[MarketTick, PriceAccumulator, VolatilityMetrics] {
    
    override def createAccumulator(): PriceAccumulator = PriceAccumulator("")
    
    override def add(tick: MarketTick, acc: PriceAccumulator): PriceAccumulator = {
      acc.symbol = tick.symbol
      acc.prices += tick.price
      acc.timestamps += tick.timestamp
      acc.volumes += tick.volume.toLong
      
      // Pour les estimateurs avancés (si disponible)
      if (tick.bid > 0 && tick.ask > 0) {
        acc.highPrices += tick.ask
        acc.lowPrices += tick.bid
      } else {
        acc.highPrices += tick.price
        acc.lowPrices += tick.price
      }
      
      acc
    }
    
    override def getResult(acc: PriceAccumulator): VolatilityMetrics = {
      if (acc.prices.size < 2) {
        return createEmptyMetrics(acc.symbol, System.currentTimeMillis())
      }
      
      val prices = acc.prices.toSeq
      val highs = acc.highPrices.toSeq
      val lows = acc.lowPrices.toSeq
      val timestamps = acc.timestamps.toSeq
      
      // Calcul des rendements logarithmiques
      val returns = prices.sliding(2).map { case Seq(p1, p2) =>
        log(p2 / p1)
      }.toSeq
      
      // Métriques de base
      val currentPrice = prices.last
      val previousPrice = prices.head
      val priceChange = currentPrice - previousPrice
      val priceChangePct = (priceChange / previousPrice) * 100
      
      // Volatilité historique (écart-type annualisé)
      val historicalVol = calculateHistoricalVolatility(returns)
      
      // Volatilité réalisée
      val realizedVol = calculateRealizedVolatility(returns)
      
      // Estimateurs avancés
      val parkinsonVol = calculateParkinsonVolatility(highs, lows)
      val garmanKlassVol = calculateGarmanKlassVolatility(prices, highs, lows)
      
      // Statistiques des rendements
      val avgReturn = returns.sum / returns.length
      val skewness = calculateSkewness(returns, avgReturn)
      val kurtosis = calculateKurtosis(returns, avgReturn)
      
      // Bandes de volatilité (±2 std dev)
      val volUpper = currentPrice + (2 * historicalVol * currentPrice)
      val volLower = currentPrice - (2 * historicalVol * currentPrice)
      
      // Métriques de risque
      val sharpe = calculateSharpeRatio(returns, avgReturn, historicalVol)
      val var95 = calculateValueAtRisk(returns, 0.95)
      val var99 = calculateValueAtRisk(returns, 0.99)
      val maxDrawdown = calculateMaxDrawdown(prices)
      
      VolatilityMetrics(
        symbol = acc.symbol,
        timestamp = System.currentTimeMillis(),
        windowStart = timestamps.head,
        windowEnd = timestamps.last,
        price = currentPrice,
        priceChange = priceChange,
        priceChangePct = priceChangePct,
        historicalVolatility = historicalVol,
        realizedVolatility = realizedVol,
        parkinsonVolatility = parkinsonVol,
        garmanKlassVolatility = garmanKlassVol,
        returns = returns,
        avgReturn = avgReturn,
        skewness = skewness,
        kurtosis = kurtosis,
        volatilityUpper = volUpper,
        volatilityLower = volLower,
        sharpeRatio = sharpe,
        valueAtRisk95 = var95,
        valueAtRisk99 = var99,
        maxDrawdown = maxDrawdown
      )
    }
    
    override def merge(a: PriceAccumulator, b: PriceAccumulator): PriceAccumulator = {
      a.prices ++= b.prices
      a.highPrices ++= b.highPrices
      a.lowPrices ++= b.lowPrices
      a.timestamps ++= b.timestamps
      a.volumes ++= b.volumes
      a
    }
  }

  /**
   * Process function pour enrichir les métriques
   */
  class VolatilityEnricher extends ProcessFunction[VolatilityMetrics, VolatilityMetrics] {
    override def processElement(
      value: VolatilityMetrics,
      ctx: ProcessFunction[VolatilityMetrics, VolatilityMetrics]#Context,
      out: Collector[VolatilityMetrics]
    ): Unit = {
      // Enrichissement futur (comparaison avec moyennes, détection de régimes, etc.)
      out.collect(value)
    }
  }

  // ============================================================
  // Fonctions de calcul de volatilité
  // ============================================================

  /**
   * Volatilité historique (écart-type des rendements)
   * Annualisée : σ_annual = σ_period * sqrt(périodes_par_an)
   */
  def calculateHistoricalVolatility(returns: Seq[Double]): Double = {
    if (returns.isEmpty) return 0.0
    
    val mean = returns.sum / returns.length
    val variance = returns.map(r => pow(r - mean, 2)).sum / returns.length
    val stdDev = sqrt(variance)
    
    // Annualiser (supposons 252 jours de trading, 390 minutes par jour)
    val periodsPerYear = 252 * 390 / 5 // 5 minutes par période
    stdDev * sqrt(periodsPerYear)
  }

  /**
   * Volatilité réalisée (somme des carrés des rendements)
   */
  def calculateRealizedVolatility(returns: Seq[Double]): Double = {
    if (returns.isEmpty) return 0.0
    
    val sumSquares = returns.map(r => r * r).sum
    val realizedVar = sumSquares
    sqrt(realizedVar)
  }

  /**
   * Estimateur de Parkinson (utilise high-low range)
   * Plus efficace que l'écart-type standard
   * σ² = (1 / (4n * ln(2))) * Σ(ln(High/Low))²
   */
  def calculateParkinsonVolatility(highs: Seq[Double], lows: Seq[Double]): Double = {
    if (highs.isEmpty || lows.isEmpty || highs.size != lows.size) return 0.0
    
    val n = highs.length
    val sumSquares = highs.zip(lows).map { case (h, l) =>
      if (l > 0) pow(log(h / l), 2) else 0.0
    }.sum
    
    val variance = sumSquares / (4 * n * log(2))
    sqrt(variance) * sqrt(252) // Annualisé
  }

  /**
   * Estimateur de Garman-Klass (utilise open, high, low, close)
   * Plus précis que Parkinson
   */
  def calculateGarmanKlassVolatility(
    closes: Seq[Double],
    highs: Seq[Double],
    lows: Seq[Double]
  ): Double = {
    if (closes.size < 2 || highs.isEmpty || lows.isEmpty) return 0.0
    
    val n = min(closes.size - 1, min(highs.size, lows.size))
    
    val variance = (0 until n).map { i =>
      val c_prev = closes(i)
      val c = closes(i + 1)
      val h = highs(i)
      val l = lows(i)
      
      if (l > 0 && c_prev > 0) {
        0.5 * pow(log(h / l), 2) - (2 * log(2) - 1) * pow(log(c / c_prev), 2)
      } else 0.0
    }.sum / n
    
    sqrt(variance) * sqrt(252) // Annualisé
  }

  /**
   * Calcul de l'asymétrie (skewness)
   * Mesure l'asymétrie de la distribution des rendements
   */
  def calculateSkewness(returns: Seq[Double], mean: Double): Double = {
    if (returns.isEmpty) return 0.0
    
    val n = returns.length
    val m3 = returns.map(r => pow(r - mean, 3)).sum / n
    val stdDev = sqrt(returns.map(r => pow(r - mean, 2)).sum / n)
    
    if (stdDev == 0) 0.0 else m3 / pow(stdDev, 3)
  }

  /**
   * Calcul du kurtosis (excès)
   * Mesure l'épaisseur des queues de distribution
   */
  def calculateKurtosis(returns: Seq[Double], mean: Double): Double = {
    if (returns.isEmpty) return 0.0
    
    val n = returns.length
    val m4 = returns.map(r => pow(r - mean, 4)).sum / n
    val variance = returns.map(r => pow(r - mean, 2)).sum / n
    
    if (variance == 0) 0.0 else (m4 / pow(variance, 2)) - 3.0 // Excès de kurtosis
  }

  /**
   * Ratio de Sharpe (rendement ajusté au risque)
   * Sharpe = (R - Rf) / σ
   * Supposons Rf = 0 pour simplifier
   */
  def calculateSharpeRatio(returns: Seq[Double], avgReturn: Double, volatility: Double): Double = {
    if (volatility == 0) return 0.0
    (avgReturn * sqrt(252)) / volatility // Annualisé
  }

  /**
   * Value at Risk (VaR) - Perte maximale probable à un niveau de confiance donné
   * Méthode historique (percentile des rendements)
   */
  def calculateValueAtRisk(returns: Seq[Double], confidenceLevel: Double): Double = {
    if (returns.isEmpty) return 0.0
    
    val sortedReturns = returns.sorted
    val index = ((1.0 - confidenceLevel) * returns.length).toInt
    
    if (index >= 0 && index < sortedReturns.length) {
      -sortedReturns(index) // Retourner en positif (perte)
    } else 0.0
  }

  /**
   * Maximum Drawdown (perte maximale depuis un pic)
   */
  def calculateMaxDrawdown(prices: Seq[Double]): Double = {
    if (prices.isEmpty) return 0.0
    
    var maxPrice = prices.head
    var maxDrawdown = 0.0
    
    prices.foreach { price =>
      maxPrice = max(maxPrice, price)
      val drawdown = (maxPrice - price) / maxPrice
      maxDrawdown = max(maxDrawdown, drawdown)
    }
    
    maxDrawdown * 100 // En pourcentage
  }

  /**
   * Créer des métriques vides en cas de données insuffisantes
   */
  def createEmptyMetrics(symbol: String, timestamp: Long): VolatilityMetrics = {
    VolatilityMetrics(
      symbol = symbol,
      timestamp = timestamp,
      windowStart = 0L,
      windowEnd = 0L,
      price = 0.0,
      priceChange = 0.0,
      priceChangePct = 0.0,
      historicalVolatility = 0.0,
      realizedVolatility = 0.0,
      parkinsonVolatility = 0.0,
      garmanKlassVolatility = 0.0,
      returns = Seq.empty,
      avgReturn = 0.0,
      skewness = 0.0,
      kurtosis = 0.0,
      volatilityUpper = 0.0,
      volatilityLower = 0.0,
      sharpeRatio = 0.0,
      valueAtRisk95 = 0.0,
      valueAtRisk99 = 0.0,
      maxDrawdown = 0.0
    )
  }
}

/**
 * Multi-timeframe volatility analysis
 */
case class MultiTimeframeVolatility(
  symbol: String,
  timestamp: Long,
  vol1min: Double,
  vol5min: Double,
  vol15min: Double,
  vol1hour: Double,
  volatilityTrend: String, // "increasing", "decreasing", "stable"
  volatilityRegime: String // "low", "medium", "high", "extreme"
)

class MultiTimeframeCombiner extends CoFlatMapFunction[VolatilityMetrics, VolatilityMetrics, MultiTimeframeVolatility] {
  // État pour stocker les volatilités de différentes timeframes
  private var vol1minMap = scala.collection.mutable.Map[String, Double]()
  private var vol5minMap = scala.collection.mutable.Map[String, Double]()
  
  override def flatMap1(
    value: VolatilityMetrics,
    out: Collector[MultiTimeframeVolatility]
  ): Unit = {
    vol1minMap(value.symbol) = value.historicalVolatility
  }
  
  override def flatMap2(
    value: VolatilityMetrics,
    out: Collector[MultiTimeframeVolatility]
  ): Unit = {
    vol5minMap(value.symbol) = value.historicalVolatility
    
    // Émettre uniquement si on a les deux timeframes
    if (vol1minMap.contains(value.symbol)) {
      val vol1 = vol1minMap(value.symbol)
      val vol5 = value.historicalVolatility
      
      // Déterminer la tendance et le régime
      val trend = if (vol5 > vol1 * 1.1) "increasing"
                  else if (vol5 < vol1 * 0.9) "decreasing"
                  else "stable"
      
      val regime = if (vol5 < 0.15) "low"
                   else if (vol5 < 0.25) "medium"
                   else if (vol5 < 0.40) "high"
                   else "extreme"
      
      out.collect(MultiTimeframeVolatility(
        symbol = value.symbol,
        timestamp = value.timestamp,
        vol1min = vol1,
        vol5min = vol5,
        vol15min = 0.0, // À compléter avec d'autres timeframes
        vol1hour = 0.0,
        volatilityTrend = trend,
        volatilityRegime = regime
      ))
    }
  }
}