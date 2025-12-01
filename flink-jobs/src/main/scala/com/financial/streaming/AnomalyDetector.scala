package com.finance.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor, ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._
import scala.math._

/**
 * Types d'anomalies détectées
 */
object AnomalyType extends Enumeration {
  type AnomalyType = Value
  val PRICE_SPIKE = Value("PRICE_SPIKE")                    // Pic de prix soudain
  val PRICE_DROP = Value("PRICE_DROP")                      // Chute de prix soudaine
  val VOLUME_SPIKE = Value("VOLUME_SPIKE")                  // Volume anormalement élevé
  val SPREAD_ANOMALY = Value("SPREAD_ANOMALY")              // Spread bid-ask anormal
  val VOLATILITY_BURST = Value("VOLATILITY_BURST")          // Explosion de volatilité
  val PRICE_FREEZE = Value("PRICE_FREEZE")                  // Prix gelé (pas de mouvement)
  val SEQUENTIAL_PATTERN = Value("SEQUENTIAL_PATTERN")      // Pattern séquentiel suspect
  val STATISTICAL_OUTLIER = Value("STATISTICAL_OUTLIER")    // Outlier statistique (Z-score)
  val FLASH_CRASH = Value("FLASH_CRASH")                    // Flash crash
  val MANIPULATION = Value("MANIPULATION")                   // Suspicion de manipulation
}

/**
 * Sévérité de l'anomalie
 */
object AnomalySeverity extends Enumeration {
  type AnomalySeverity = Value
  val LOW = Value("LOW")
  val MEDIUM = Value("MEDIUM")
  val HIGH = Value("HIGH")
  val CRITICAL = Value("CRITICAL")
}

/**
 * Case class pour une anomalie détectée
 */
case class MarketAnomaly(
  symbol: String,
  timestamp: Long,
  anomalyType: AnomalyType.Value,
  severity: AnomalySeverity.Value,
  currentPrice: Double,
  expectedPrice: Double,
  deviationPct: Double,
  volume: Long,
  avgVolume: Long,
  zScore: Double,
  confidence: Double,
  description: String,
  metadata: Map[String, String] = Map.empty
)

/**
 * État pour le tracking des statistiques
 */
case class AnomalyState(
  symbol: String,
  recentPrices: Seq[Double] = Seq.empty,
  recentVolumes: Seq[Long] = Seq.empty,
  recentTimestamps: Seq[Long] = Seq.empty,
  meanPrice: Double = 0.0,
  stdDevPrice: Double = 0.0,
  meanVolume: Double = 0.0,
  stdDevVolume: Double = 0.0,
  lastUpdateTime: Long = 0L,
  consecutiveFreezes: Int = 0,
  lastPrice: Double = 0.0
)

/**
 * Détecteur d'anomalies multi-méthodes
 */
object AnomalyDetector {

  // Seuils de détection
  val PRICE_SPIKE_THRESHOLD = 3.0        // Z-score > 3
  val VOLUME_SPIKE_THRESHOLD = 3.5       // Z-score > 3.5
  val SPREAD_THRESHOLD = 0.05            // 5% du prix
  val VOLATILITY_THRESHOLD = 0.50        // 50% de volatilité
  val FREEZE_THRESHOLD = 60000L          // 60 secondes sans mouvement
  val FLASH_CRASH_THRESHOLD = -0.10      // -10% en quelques secondes
  val MANIPULATION_PATTERN_LENGTH = 5

  /**
   * Pipeline principal de détection d'anomalies
   */
  def detectAnomalies(
    marketStream: DataStream[MarketTick],
    windowSize: Time = Time.minutes(10),
    slideSize: Time = Time.seconds(30)
  ): DataStream[MarketAnomaly] = {
    
    marketStream
      .keyBy(_.symbol)
      .process(new StatisticalAnomalyDetector())
      .filter(_.isDefined)
      .map(_.get)
  }

  /**
   * Détecteur d'anomalies statistiques avec état
   */
  class StatisticalAnomalyDetector 
    extends KeyedProcessFunction[String, MarketTick, Option[MarketAnomaly]] {
    
    // États Flink pour maintenir les statistiques
    private var stateDescriptor: ValueStateDescriptor[AnomalyState] = _
    private var anomalyState: ValueState[AnomalyState] = _
    
    // Buffer circulaire pour les valeurs récentes
    private val BUFFER_SIZE = 100
    
    override def open(parameters: Configuration): Unit = {
      stateDescriptor = new ValueStateDescriptor[AnomalyState](
        "anomaly-state",
        classOf[AnomalyState]
      )
      anomalyState = getRuntimeContext.getState(stateDescriptor)
    }
    
    override def processElement(
      tick: MarketTick,
      ctx: KeyedProcessFunction[String, MarketTick, Option[MarketAnomaly]]#Context,
      out: Collector[Option[MarketAnomaly]]
    ): Unit = {
      
      // Récupérer ou initialiser l'état
      var state = Option(anomalyState.value()).getOrElse(
        AnomalyState(symbol = tick.symbol, lastPrice = tick.price)
      )
      
      // Mettre à jour le buffer circulaire
      state = updateStateBuffer(state, tick)
      
      // Recalculer les statistiques
      state = updateStatistics(state)
      
      // Exécuter tous les détecteurs
      val anomalies = detectAllAnomalies(tick, state)
      
      // Émettre les anomalies détectées
      anomalies.foreach { anomaly =>
        out.collect(Some(anomaly))
      }
      
      // Sauvegarder l'état mis à jour
      anomalyState.update(state)
      
      // Si aucune anomalie, émettre None
      if (anomalies.isEmpty) {
        out.collect(None)
      }
    }
    
    /**
     * Mettre à jour le buffer d'état
     */
    def updateStateBuffer(state: AnomalyState, tick: MarketTick): AnomalyState = {
      val newPrices = (state.recentPrices :+ tick.price).takeRight(BUFFER_SIZE)
      val newVolumes = (state.recentVolumes :+ tick.volume.toLong).takeRight(BUFFER_SIZE)
      val newTimestamps = (state.recentTimestamps :+ tick.timestamp).takeRight(BUFFER_SIZE)
      
      state.copy(
        recentPrices = newPrices,
        recentVolumes = newVolumes,
        recentTimestamps = newTimestamps,
        lastUpdateTime = tick.timestamp,
        lastPrice = tick.price
      )
    }
    
    /**
     * Recalculer les statistiques (moyenne, écart-type)
     */
    def updateStatistics(state: AnomalyState): AnomalyState = {
      if (state.recentPrices.isEmpty) return state
      
      val meanPrice = state.recentPrices.sum / state.recentPrices.length
      val variancePrice = state.recentPrices.map(p => pow(p - meanPrice, 2)).sum / state.recentPrices.length
      val stdDevPrice = sqrt(variancePrice)
      
      val meanVolume = state.recentVolumes.sum.toDouble / state.recentVolumes.length
      val varianceVolume = state.recentVolumes.map(v => pow(v - meanVolume, 2)).sum / state.recentVolumes.length
      val stdDevVolume = sqrt(varianceVolume)
      
      state.copy(
        meanPrice = meanPrice,
        stdDevPrice = stdDevPrice,
        meanVolume = meanVolume,
        stdDevVolume = stdDevVolume
      )
    }
    
    /**
     * Exécuter tous les détecteurs d'anomalies
     */
    def detectAllAnomalies(tick: MarketTick, state: AnomalyState): Seq[MarketAnomaly] = {
      val anomalies = scala.collection.mutable.ListBuffer[MarketAnomaly]()
      
      // 1. Détection de pic/chute de prix (Z-score)
      detectPriceAnomaly(tick, state).foreach(anomalies += _)
      
      // 2. Détection de volume anormal
      detectVolumeAnomaly(tick, state).foreach(anomalies += _)
      
      // 3. Détection de spread anormal
      detectSpreadAnomaly(tick, state).foreach(anomalies += _)
      
      // 4. Détection de prix gelé
      detectPriceFreeze(tick, state).foreach(anomalies += _)
      
      // 5. Détection de flash crash
      detectFlashCrash(tick, state).foreach(anomalies += _)
      
      // 6. Détection de patterns de manipulation
      detectManipulationPattern(tick, state).foreach(anomalies += _)
      
      anomalies.toSeq
    }
    
    /**
     * 1. Détection d'anomalie de prix (Z-score)
     */
    def detectPriceAnomaly(tick: MarketTick, state: AnomalyState): Option[MarketAnomaly] = {
      if (state.stdDevPrice == 0 || state.recentPrices.length < 10) return None
      
      val zScore = (tick.price - state.meanPrice) / state.stdDevPrice
      
      if (abs(zScore) > PRICE_SPIKE_THRESHOLD) {
        val anomalyType = if (zScore > 0) AnomalyType.PRICE_SPIKE else AnomalyType.PRICE_DROP
        val deviationPct = ((tick.price - state.meanPrice) / state.meanPrice) * 100
        
        val severity = if (abs(zScore) > 5.0) AnomalySeverity.CRITICAL
                       else if (abs(zScore) > 4.0) AnomalySeverity.HIGH
                       else AnomalySeverity.MEDIUM
        
        Some(MarketAnomaly(
          symbol = tick.symbol,
          timestamp = tick.timestamp,
          anomalyType = anomalyType,
          severity = severity,
          currentPrice = tick.price,
          expectedPrice = state.meanPrice,
          deviationPct = deviationPct,
          volume = tick.volume.toLong,
          avgVolume = state.meanVolume.toLong,
          zScore = zScore,
          confidence = min(abs(zScore) / 5.0, 1.0),
          description = s"${anomalyType} detected: Price ${tick.price} deviates ${deviationPct.formatted("%.2f")}% from mean ${state.meanPrice.formatted("%.2f")} (Z-score: ${zScore.formatted("%.2f")})"
        ))
      } else None
    }
    
    /**
     * 2. Détection de volume anormal
     */
    def detectVolumeAnomaly(tick: MarketTick, state: AnomalyState): Option[MarketAnomaly] = {
      if (state.stdDevVolume == 0 || state.recentVolumes.length < 10) return None
      
      val zScore = (tick.volume - state.meanVolume) / state.stdDevVolume
      
      if (zScore > VOLUME_SPIKE_THRESHOLD) {
        val deviationPct = ((tick.volume - state.meanVolume) / state.meanVolume) * 100
        
        val severity = if (zScore > 6.0) AnomalySeverity.CRITICAL
                       else if (zScore > 4.5) AnomalySeverity.HIGH
                       else AnomalySeverity.MEDIUM
        
        Some(MarketAnomaly(
          symbol = tick.symbol,
          timestamp = tick.timestamp,
          anomalyType = AnomalyType.VOLUME_SPIKE,
          severity = severity,
          currentPrice = tick.price,
          expectedPrice = state.meanPrice,
          deviationPct = deviationPct,
          volume = tick.volume.toLong,
          avgVolume = state.meanVolume.toLong,
          zScore = zScore,
          confidence = min(zScore / 6.0, 1.0),
          description = s"Volume spike: ${tick.volume} is ${deviationPct.formatted("%.0f")}% above average ${state.meanVolume.formatted("%.0f")} (Z-score: ${zScore.formatted("%.2f")})"
        ))
      } else None
    }
    
    /**
     * 3. Détection de spread bid-ask anormal
     */
    def detectSpreadAnomaly(tick: MarketTick, state: AnomalyState): Option[MarketAnomaly] = {
      if (tick.bid <= 0 || tick.ask <= 0) return None
      
      val spread = tick.ask - tick.bid
      val spreadPct = spread / tick.price
      
      if (spreadPct > SPREAD_THRESHOLD) {
        val severity = if (spreadPct > 0.10) AnomalySeverity.HIGH
                       else if (spreadPct > 0.07) AnomalySeverity.MEDIUM
                       else AnomalySeverity.LOW
        
        Some(MarketAnomaly(
          symbol = tick.symbol,
          timestamp = tick.timestamp,
          anomalyType = AnomalyType.SPREAD_ANOMALY,
          severity = severity,
          currentPrice = tick.price,
          expectedPrice = state.meanPrice,
          deviationPct = spreadPct * 100,
          volume = tick.volume.toLong,
          avgVolume = state.meanVolume.toLong,
          zScore = 0.0,
          confidence = min(spreadPct / 0.10, 1.0),
          description = s"Abnormal spread: ${(spreadPct * 100).formatted("%.2f")}% (bid: ${tick.bid}, ask: ${tick.ask})",
          metadata = Map("bid" -> tick.bid.toString, "ask" -> tick.ask.toString, "spread" -> spread.toString)
        ))
      } else None
    }
    
    /**
     * 4. Détection de prix gelé (pas de mouvement)
     */
    def detectPriceFreeze(tick: MarketTick, state: AnomalyState): Option[MarketAnomaly] = {
      if (state.recentPrices.length < 5) return None
      
      val recentUniquePrices = state.recentPrices.takeRight(10).distinct
      
      if (recentUniquePrices.length == 1 && state.recentPrices.length >= 10) {
        Some(MarketAnomaly(
          symbol = tick.symbol,
          timestamp = tick.timestamp,
          anomalyType = AnomalyType.PRICE_FREEZE,
          severity = AnomalySeverity.MEDIUM,
          currentPrice = tick.price,
          expectedPrice = state.meanPrice,
          deviationPct = 0.0,
          volume = tick.volume.toLong,
          avgVolume = state.meanVolume.toLong,
          zScore = 0.0,
          confidence = 0.8,
          description = s"Price frozen at ${tick.price} for ${state.recentPrices.takeRight(10).length} consecutive ticks"
        ))
      } else None
    }
    
    /**
     * 5. Détection de flash crash (chute brutale)
     */
    def detectFlashCrash(tick: MarketTick, state: AnomalyState): Option[MarketAnomaly] = {
      if (state.recentPrices.length < 5) return None
      
      val recentMax = state.recentPrices.takeRight(10).max
      val priceDrop = (tick.price - recentMax) / recentMax
      
      if (priceDrop < FLASH_CRASH_THRESHOLD) {
        Some(MarketAnomaly(
          symbol = tick.symbol,
          timestamp = tick.timestamp,
          anomalyType = AnomalyType.FLASH_CRASH,
          severity = AnomalySeverity.CRITICAL,
          currentPrice = tick.price,
          expectedPrice = recentMax,
          deviationPct = priceDrop * 100,
          volume = tick.volume.toLong,
          avgVolume = state.meanVolume.toLong,
          zScore = 0.0,
          confidence = 0.95,
          description = s"Flash crash detected: Price dropped ${(priceDrop * 100).formatted("%.2f")}% from recent high ${recentMax.formatted("%.2f")}",
          metadata = Map("recent_high" -> recentMax.toString, "drop_pct" -> (priceDrop * 100).toString)
        ))
      } else None
    }
    
    /**
     * 6. Détection de patterns de manipulation (séquences suspectes)
     */
    def detectManipulationPattern(tick: MarketTick, state: AnomalyState): Option[MarketAnomaly] = {
      if (state.recentPrices.length < MANIPULATION_PATTERN_LENGTH) return None
      
      val recentPrices = state.recentPrices.takeRight(MANIPULATION_PATTERN_LENGTH)
      
      // Pattern 1: Mouvement parfaitement linéaire (suspect)
      val isLinear = checkLinearPattern(recentPrices)
      
      // Pattern 2: Alternance up/down régulière (pump and dump)
      val isAlternating = checkAlternatingPattern(recentPrices)
      
      if (isLinear || isAlternating) {
        val patternType = if (isLinear) "linear" else "alternating"
        
        Some(MarketAnomaly(
          symbol = tick.symbol,
          timestamp = tick.timestamp,
          anomalyType = AnomalyType.MANIPULATION,
          severity = AnomalySeverity.HIGH,
          currentPrice = tick.price,
          expectedPrice = state.meanPrice,
          deviationPct = 0.0,
          volume = tick.volume.toLong,
          avgVolume = state.meanVolume.toLong,
          zScore = 0.0,
          confidence = 0.7,
          description = s"Suspicious ${patternType} trading pattern detected",
          metadata = Map("pattern_type" -> patternType)
        ))
      } else None
    }
    
    /**
     * Vérifier si les prix suivent un pattern linéaire parfait
     */
    def checkLinearPattern(prices: Seq[Double]): Boolean = {
      if (prices.length < 3) return false
      
      val diffs = prices.sliding(2).map { case Seq(a, b) => b - a }.toSeq
      val avgDiff = diffs.sum / diffs.length
      
      // Si tous les mouvements sont presque identiques
      val variance = diffs.map(d => pow(d - avgDiff, 2)).sum / diffs.length
      variance < 0.0001 && avgDiff != 0
    }
    
    /**
     * Vérifier un pattern alternant (up, down, up, down...)
     */
    def checkAlternatingPattern(prices: Seq[Double]): Boolean = {
      if (prices.length < 4) return false
      
      val directions = prices.sliding(2).map {
        case Seq(a, b) => if (b > a) 1 else if (b < a) -1 else 0
      }.toSeq
      
      // Vérifier l'alternance parfaite
      directions.sliding(2).forall {
        case Seq(a, b) => a != 0 && b != 0 && a != b
        case _ => false
      }
    }
  }

  /**
   * Enrichir les anomalies avec des informations contextuelles
   */
  class AnomalyEnricher extends ProcessFunction[MarketAnomaly, MarketAnomaly] {
    override def processElement(
      anomaly: MarketAnomaly,
      ctx: ProcessFunction[MarketAnomaly, MarketAnomaly]#Context,
      out: Collector[MarketAnomaly]
    ): Unit = {
      // Enrichissement futur: corrélation avec news, événements du marché, etc.
      out.collect(anomaly)
    }
  }
}