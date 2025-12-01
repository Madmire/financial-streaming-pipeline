package com.finance.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor, ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._
import scala.math._

/**
 * Indicateurs techniques calculés
 */
case class TechnicalIndicatorMetrics(
  symbol: String,
  timestamp: Long,
  price: Double,
  // Moving Averages
  sma_20: Double,
  sma_50: Double,
  sma_200: Double,
  ema_12: Double,
  ema_26: Double,
  // Momentum
  rsi_14: Double,
  macd: Double,
  macd_signal: Double,
  macd_histogram: Double,
  // Bollinger Bands
  bollinger_upper: Double,
  bollinger_middle: Double,
  bollinger_lower: Double,
  // Volume
  volume_sma_20: Double,
  // Stochastic
  stochastic_k: Double,
  stochastic_d: Double
)

/**
 * État pour le calcul des indicateurs
 */
case class IndicatorState(
  symbol: String,
  prices: Seq[Double] = Seq.empty,
  volumes: Seq[Long] = Seq.empty,
  ema_12_prev: Double = 0.0,
  ema_26_prev: Double = 0.0,
  macd_signal_prev: Double = 0.0,
  lastUpdateTime: Long = 0L
)

/**
 * Calculateur d'indicateurs techniques
 */
object TechnicalIndicators {

  val BUFFER_SIZE = 200 // Pour SMA 200

  /**
   * Pipeline principal de calcul des indicateurs
   */
  def calculate(marketStream: DataStream[MarketTick]): DataStream[TechnicalIndicatorMetrics] = {
    marketStream
      .keyBy(_.symbol)
      .process(new TechnicalIndicatorCalculator())
  }

  /**
   * Process function pour calculer tous les indicateurs
   */
  class TechnicalIndicatorCalculator 
    extends KeyedProcessFunction[String, MarketTick, TechnicalIndicatorMetrics] {
    
    private var stateDescriptor: ValueStateDescriptor[IndicatorState] = _
    private var indicatorState: ValueState[IndicatorState] = _
    
    override def open(parameters: Configuration): Unit = {
      stateDescriptor = new ValueStateDescriptor[IndicatorState](
        "indicator-state",
        classOf[IndicatorState]
      )
      indicatorState = getRuntimeContext.getState(stateDescriptor)
    }
    
    override def processElement(
      tick: MarketTick,
      ctx: KeyedProcessFunction[String, MarketTick, TechnicalIndicatorMetrics]#Context,
      out: Collector[TechnicalIndicatorMetrics]
    ): Unit = {
      
      // Récupérer ou initialiser l'état
      var state = Option(indicatorState.value()).getOrElse(
        IndicatorState(symbol = tick.symbol)
      )
      
      // Mettre à jour le buffer
      state = updateBuffer(state, tick)
      
      // Calculer tous les indicateurs
      val indicators = calculateAllIndicators(state, tick)
      
      // Sauvegarder l'état
      indicatorState.update(state)
      
      // Émettre les indicateurs
      out.collect(indicators)
    }
    
    /**
     * Mettre à jour le buffer de données
     */
    def updateBuffer(state: IndicatorState, tick: MarketTick): IndicatorState = {
      state.copy(
        prices = (state.prices :+ tick.price).takeRight(BUFFER_SIZE),
        volumes = (state.volumes :+ tick.volume.toLong).takeRight(BUFFER_SIZE),
        lastUpdateTime = tick.timestamp
      )
    }
    
    /**
     * Calculer tous les indicateurs
     */
    def calculateAllIndicators(
      state: IndicatorState, 
      tick: MarketTick
    ): TechnicalIndicatorMetrics = {
      
      val prices = state.prices
      
      // Moving Averages
      val sma20 = calculateSMA(prices, 20)
      val sma50 = calculateSMA(prices, 50)
      val sma200 = calculateSMA(prices, 200)
      
      // Exponential Moving Averages
      val ema12 = calculateEMA(tick.price, state.ema_12_prev, 12, prices.length)
      val ema26 = calculateEMA(tick.price, state.ema_26_prev, 26, prices.length)
      
      // MACD
      val macd = ema12 - ema26
      val macdSignal = calculateEMA(macd, state.macd_signal_prev, 9, prices.length)
      val macdHistogram = macd - macdSignal
      
      // RSI
      val rsi = calculateRSI(prices, 14)
      
      // Bollinger Bands
      val (bbUpper, bbMiddle, bbLower) = calculateBollingerBands(prices, 20, 2.0)
      
      // Volume SMA
      val volumeSMA = if (state.volumes.nonEmpty) {
        state.volumes.takeRight(20).sum.toDouble / min(state.volumes.length, 20)
      } else 0.0
      
      // Stochastic Oscillator
      val (stochK, stochD) = calculateStochastic(prices, 14, 3)
      
      // Mettre à jour l'état avec les nouvelles EMA
      indicatorState.update(state.copy(
        ema_12_prev = ema12,
        ema_26_prev = ema26,
        macd_signal_prev = macdSignal
      ))
      
      TechnicalIndicatorMetrics(
        symbol = tick.symbol,
        timestamp = tick.timestamp,
        price = tick.price,
        sma_20 = sma20,
        sma_50 = sma50,
        sma_200 = sma200,
        ema_12 = ema12,
        ema_26 = ema26,
        rsi_14 = rsi,
        macd = macd,
        macd_signal = macdSignal,
        macd_histogram = macdHistogram,
        bollinger_upper = bbUpper,
        bollinger_middle = bbMiddle,
        bollinger_lower = bbLower,
        volume_sma_20 = volumeSMA,
        stochastic_k = stochK,
        stochastic_d = stochD
      )
    }
  }

  // ============================================================
  // Fonctions de calcul des indicateurs
  // ============================================================

  /**
   * Simple Moving Average (SMA)
   */
  def calculateSMA(prices: Seq[Double], period: Int): Double = {
    if (prices.length < period) return 0.0
    prices.takeRight(period).sum / period
  }

  /**
   * Exponential Moving Average (EMA)
   * EMA = Price(t) * k + EMA(y) * (1 - k)
   * k = 2 / (N + 1)
   */
  def calculateEMA(
    currentPrice: Double, 
    previousEMA: Double, 
    period: Int,
    dataPoints: Int
  ): Double = {
    if (dataPoints < period) return currentPrice
    if (previousEMA == 0.0) return currentPrice
    
    val k = 2.0 / (period + 1)
    currentPrice * k + previousEMA * (1 - k)
  }

  /**
   * Relative Strength Index (RSI)
   * RSI = 100 - (100 / (1 + RS))
   * RS = Average Gain / Average Loss
   */
  def calculateRSI(prices: Seq[Double], period: Int = 14): Double = {
    if (prices.length < period + 1) return 50.0 // Neutre
    
    val changes = prices.sliding(2).map { case Seq(p1, p2) => p2 - p1 }.toSeq
    val recentChanges = changes.takeRight(period)
    
    val gains = recentChanges.filter(_ > 0)
    val losses = recentChanges.filter(_ < 0).map(abs)
    
    if (losses.isEmpty) return 100.0
    if (gains.isEmpty) return 0.0
    
    val avgGain = gains.sum / period
    val avgLoss = losses.sum / period
    
    if (avgLoss == 0.0) return 100.0
    
    val rs = avgGain / avgLoss
    100.0 - (100.0 / (1.0 + rs))
  }

  /**
   * Bollinger Bands
   * Middle Band = SMA(N)
   * Upper Band = Middle + (k * stddev)
   * Lower Band = Middle - (k * stddev)
   */
  def calculateBollingerBands(
    prices: Seq[Double], 
    period: Int = 20, 
    numStdDev: Double = 2.0
  ): (Double, Double, Double) = {
    if (prices.length < period) return (0.0, 0.0, 0.0)
    
    val recentPrices = prices.takeRight(period)
    val middle = recentPrices.sum / period
    
    val variance = recentPrices.map(p => pow(p - middle, 2)).sum / period
    val stdDev = sqrt(variance)
    
    val upper = middle + (numStdDev * stdDev)
    val lower = middle - (numStdDev * stdDev)
    
    (upper, middle, lower)
  }

  /**
   * MACD (Moving Average Convergence Divergence)
   * Calculé via les EMA dans le process function
   */

  /**
   * Stochastic Oscillator
   * %K = (Close - Low(n)) / (High(n) - Low(n)) * 100
   * %D = SMA(%K, 3)
   */
  def calculateStochastic(
    prices: Seq[Double], 
    period: Int = 14, 
    smoothK: Int = 3
  ): (Double, Double) = {
    if (prices.length < period) return (50.0, 50.0)
    
    val recentPrices = prices.takeRight(period)
    val currentPrice = recentPrices.last
    val highest = recentPrices.max
    val lowest = recentPrices.min
    
    val stochK = if (highest - lowest == 0.0) {
      50.0
    } else {
      ((currentPrice - lowest) / (highest - lowest)) * 100.0
    }
    
    // Pour %D, on aurait besoin de stocker les %K précédents
    // Simplification : %D = %K pour l'instant
    val stochD = stochK
    
    (stochK, stochD)
  }

  /**
   * Average True Range (ATR) - Mesure de volatilité
   */
  def calculateATR(
    highs: Seq[Double], 
    lows: Seq[Double], 
    closes: Seq[Double], 
    period: Int = 14
  ): Double = {
    if (highs.length < period + 1 || lows.length < period + 1 || closes.length < period + 1) {
      return 0.0
    }
    
    val trueRanges = (1 until period + 1).map { i =>
      val high = highs(highs.length - i)
      val low = lows(lows.length - i)
      val prevClose = closes(closes.length - i - 1)
      
      max(max(high - low, abs(high - prevClose)), abs(low - prevClose))
    }
    
    trueRanges.sum / period
  }

  /**
   * On-Balance Volume (OBV)
   */
  def calculateOBV(prices: Seq[Double], volumes: Seq[Long]): Long = {
    if (prices.length < 2 || volumes.isEmpty) return 0L
    
    prices.zip(volumes).sliding(2).map {
      case Seq((p1, v1), (p2, v2)) =>
        if (p2 > p1) v2
        else if (p2 < p1) -v2
        else 0L
    }.sum
  }

  /**
   * Commodity Channel Index (CCI)
   */
  def calculateCCI(
    prices: Seq[Double],
    highs: Seq[Double],
    lows: Seq[Double],
    period: Int = 20
  ): Double = {
    if (prices.length < period) return 0.0
    
    // Typical Price = (High + Low + Close) / 3
    val typicalPrices = (0 until min(prices.length, min(highs.length, lows.length))).map { i =>
      (highs(i) + lows(i) + prices(i)) / 3.0
    }
    
    val recentTP = typicalPrices.takeRight(period)
    val smaTP = recentTP.sum / period
    
    // Mean Deviation
    val meanDev = recentTP.map(tp => abs(tp - smaTP)).sum / period
    
    if (meanDev == 0.0) return 0.0
    
    val currentTP = recentTP.last
    ((currentTP - smaTP) / (0.015 * meanDev))
  }

  /**
   * Ichimoku Cloud (simplifié)
   */
  def calculateIchimoku(
    highs: Seq[Double],
    lows: Seq[Double],
    period1: Int = 9,
    period2: Int = 26,
    period3: Int = 52
  ): (Double, Double, Double) = {
    if (highs.length < period3 || lows.length < period3) {
      return (0.0, 0.0, 0.0)
    }
    
    // Tenkan-sen (Conversion Line) = (9-period high + 9-period low) / 2
    val tenkan = (highs.takeRight(period1).max + lows.takeRight(period1).min) / 2.0
    
    // Kijun-sen (Base Line) = (26-period high + 26-period low) / 2
    val kijun = (highs.takeRight(period2).max + lows.takeRight(period2).min) / 2.0
    
    // Senkou Span B = (52-period high + 52-period low) / 2
    val senkouB = (highs.takeRight(period3).max + lows.takeRight(period3).min) / 2.0
    
    (tenkan, kijun, senkouB)
  }
}