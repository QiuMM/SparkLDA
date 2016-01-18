package com.qmm.ml

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDAModel}

/**
  * Created by qmm on 2015/12/9.
  */
class PrintDistributions(ldaModel: LDAModel, vocabArray: Array[String], docMap: Map[Long, String]) {

  def print(): Unit = {
    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }

    println(s"${Params.getK} topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }

    // Print the document-topic distribution, showing the top-weighted topics for each document.
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        val documentIndices = distLDAModel.topTopicsPerDocument(3).map { case (docID, topicIndices, topicWeights) =>
          (docID, topicIndices.zip(topicWeights))
        }.take(6)

        println("six documents: ")
        for (i <- 0 to 5) {
          val docToTopic = documentIndices(i)
          println(docToTopic._1 + "(" + docMap(docToTopic._1) + ")")
          docToTopic._2.foreach {
            case (index, weight) =>
              println(s"$index\t$weight")
          }
          println()
        }
      case _ =>
    }
  }
}
