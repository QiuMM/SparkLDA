package com.qmm.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by qmm on 2015/12/9.
  */
object Lda {

  def main(args: Array[String]) {
    run()
  }

  private def run() {
    val conf = new SparkConf().setAppName("LDAModel").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // Load documents, and prepare them for LDA.
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens, docMap) = preprocess(sc, Params.getInputDir, Params.getVocabSize, Params.getStopwordFile)
    corpus.cache()
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.length
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9

    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println("\t Training MaxIterations: " + Params.getMaxIterations)
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println()

    // training LDA model
    val lda = new LDA()

    val optimizer = Params.getAlgorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(s"Only em, online are supported but got ${Params.getAlgorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(Params.getK)
      .setMaxIterations(Params.getMaxIterations)
      .setDocConcentration(Params.getDocConcentration)
      .setTopicConcentration(Params.getTopicConcentration)
    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.  Summary:")
    println(s"\t Training time: $elapsed sec")

    // print distributions
    val pd = new PrintDistributions(ldaModel, vocabArray, docMap)
    pd.print()

    // forecast
    forecast(sc, ldaModel.asInstanceOf[DistributedLDAModel].toLocal, vocabArray, Params.getForecastDir)

    // topic query
    val tq = new TopicQuery(ldaModel, docMap)
    tq.query(0)

    //ldaModel.save(sc, "myLDAModel")
    sc.stop()
  }

  /**
    * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
    * @return (corpus, vocabulary as array, total token count in corpus, key-value pair [docId, docPath] as a map)
    */
  private def preprocess(
                          sc: SparkContext,
                          inputDir: String,
                          vocabSize: Int,
                          stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long, Map[Long, String]) = {

    val textRDD = sc.wholeTextFiles(inputDir)

    // Split text into words
    val tokenizer = new SimpleTokenizer(sc, stopwordFile)
    // key-value pair [docId, docPath]
    val docMap = textRDD.zipWithIndex.map { case (text, id) =>
      id -> text._1
    }.collectAsMap().toMap

    val tokenized = textRDD.zipWithIndex.map { case (text, id) =>
      id -> tokenizer.getWords(text._2)
    }
    tokenized.cache()

    // Counts words
    val wordCounts = tokenized
      .flatMap { case (id, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()
    val fullVocabSize = wordCounts.count()
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)
    val (vocab: Map[String, Int], selectedTokenCount: Long) = {
      val tmpSortedWC = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
      }
      (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
    }

    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = Vectors.sparse(vocab.size, indices, values)
      (id, sb)
    }

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }

    (documents, vocabArray, selectedTokenCount, docMap)
  }

  private def forecast(sc: SparkContext, localModel: LocalLDAModel, vocabArray: Array[String], forecastDir: String): Unit = {
    val forecastText = sc.wholeTextFiles(forecastDir)
    val tokenizer = new SimpleTokenizer(sc, Params.getStopwordFile)
    val tokenized = forecastText.zipWithIndex.map { case (text, id) =>
      id -> tokenizer.getWords(text._2)
    }
    tokenized.cache()

    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocabArray.contains(term)) {
          val termIndex = vocabArray.indexOf(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = Vectors.sparse(vocabArray.length, indices, values)
      (id, sb)
    }

    val docTopicsWeight = localModel.topicDistributions(documents).collect()

    // print topic distribution for the new documents
    docTopicsWeight.foreach { case (id, v) =>
      val da = v.toArray
      for (elem <- da) {
        print(elem + ", ")
      }
      println()
    }

    /*// compute accuracy rate
    var i = 0d
    docTopicsWeight.foreach { case (id, v) =>
      val da = v.toArray
      val index = da.indexOf(da.max)
      if (index == topicIndex) {
        i = i + 1
      }
    }
    val rate = i / docTopicsWeight.length
    println(rate)*/
  }
}