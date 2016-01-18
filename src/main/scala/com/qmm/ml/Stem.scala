package com.qmm.ml

import edu.stanford.nlp.process.Morphology

/**
  * Created by qmm on 2015/12/25.
  */
class Stem extends Serializable {

  def stem(word: String): String = {

    val morph = new Morphology()
    morph.stem(word)
  }
}
