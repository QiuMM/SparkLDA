package com.qmm.ml

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by qmm on 2015/12/9.
  */
object Params {
  private val PARAMS_FILE_PATH = System.getProperty("user.dir") + "/" + "params.properties"
  private val INPUT_DIR = "inputDir"
  private val K = "k"
  private val MAX_ITERATIONS = "maxIterations"
  private val DOC_CONCENTRATION = "docConcentration"
  private val TOPIC_CONCENTRATION = "topicConcentration"
  private val VOCAB_SIZE = "vocabSize"
  private val STOPWORD_FILE = "stopwordFile"
  private val ALGORITHM = "algorithm"
  private val CHECKPOINT_DIR = "checkpointDir"
  private val CHECKPOINT_INTERVAL = "checkpointInterval"
  private val MODEL_PATH = "modelPath"
  private val FORECAST_DIR = "forecastDir"

  private var inputDir: String = "hdfs://pc53:9000/corpus"
  private var k: Int = 20
  private var maxIterations: Int = 10
  private var docConcentration: Double = -1
  private var topicConcentration: Double = -1
  private var vocabSize: Int = 10000
  private var stopwordFile: String = "hdfs://pc53:9000/input/stop.txt"
  private var algorithm: String = "em"
  private var checkpointDir: String= "None"
  private var checkpointInterval: Int = 10
  private var modelPath: String = "hdfs://pc53:9000/output/myLDAModel"
  private var forecastDir: String = "hdfs://pc53:9000/forecast"

  try {
    val props = new Properties
    props.load(new FileInputStream(PARAMS_FILE_PATH))

    inputDir = props.getProperty(INPUT_DIR)
    k = props.getProperty(K).toInt
    maxIterations = props.getProperty(MAX_ITERATIONS).toInt
    docConcentration = props.getProperty(DOC_CONCENTRATION).toDouble
    topicConcentration = props.getProperty(TOPIC_CONCENTRATION).toDouble
    vocabSize = props.getProperty(VOCAB_SIZE).toInt
    stopwordFile = props.getProperty(STOPWORD_FILE)
    algorithm = props.getProperty(ALGORITHM)
    checkpointDir =  props.getProperty(CHECKPOINT_DIR)
    checkpointInterval = props.getProperty(CHECKPOINT_INTERVAL).toInt
    modelPath = props.getProperty(MODEL_PATH)
    forecastDir = props.getProperty(FORECAST_DIR)
  } catch {
    case e: Exception => e.printStackTrace()
      print(e.getMessage)
  }

  def getInputDir = inputDir
  def getK = k
  def getMaxIterations = maxIterations
  def getDocConcentration = docConcentration
  def getTopicConcentration = topicConcentration
  def getVocabSize = vocabSize
  def getStopwordFile = stopwordFile
  def getAlgorithm = algorithm
  def getCheckpointDir = checkpointDir
  def getCheckpointInterval = checkpointInterval
  def getModelPath = modelPath
  def getForecastDir = forecastDir
}
