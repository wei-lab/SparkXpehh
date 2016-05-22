/*
 * AlgorithmMemory.scala for SparkXpehh
 * Copyright (c) 2015-2016 Wei Zhou, Changchun Liu, Haibin Xie All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package com.ynu.algorithm

import scala.collection.mutable.HashMap

/**
 * @author liucc
 * 
 * 采用内存方式缓存EHH
 */
class AlgorithmMemory(snpMap: Map[Int, Int], pedMapList: List[(String, (String, String))]) extends AlgorithmAbstract(snpMap, pedMapList) {

  /**
   * 缓存ehh，ehh在事先计算完毕，缓存在内存中
   */
  def cacheSampleEhh(index: Int, sampleEHHMap: HashMap[String, String]) = {

    // 将cutoff之间的的ehh存储为[起始位置，值]
    sampleEHHMap.map(x => {
      if (x._1.toInt < index) {
        // index左侧值
        ehhMap.put(x._1 + "-" + index, x._2)
      } else {
        // index右侧值
        ehhMap.put(index + "-" + x._1, x._2)
      }
    })
  }
}