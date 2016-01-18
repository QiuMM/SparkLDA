package com.qmm.ml

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDAModel}

/**
  * Created by qmm on 2015/12/9.
  */
class TopicQuery(ldaModel: LDAModel, docMap: Map[Long, String]) {

  def query(topicIndex: Int): Unit = {
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        val documentIndices = distLDAModel.topDocumentsPerTopic(10).map { case (docIds, topicWeights) =>
          docIds.zip(topicWeights)
        }
        val docs = documentIndices(topicIndex)

        docs.foreach { case (docId, topicWeight) =>
          print(docId + "(" + docMap(docId) + ")")
          println(s"\t$topicWeight")
        }
      case _ =>
    }
  }
}
