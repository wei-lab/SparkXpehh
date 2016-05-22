/*
 * AlgorithmAbstract.scala for SparkXpehh
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
 * xp-ehh 关键算法基类
 */
abstract class AlgorithmAbstract(snpMap: Map[Int, Int], pedMapList: List[(String, (String, String))]) extends Serializable {

  val snpCount = snpMap.size
  val pedCount = pedMapList.size

  // ---缓存------------
  val REDIS_HM_EHH = "ehhmap"
  val REDIS_HM_LEFT_IHH1 = "ihhmap-left1"
  val REDIS_HM_LEFT_IHH2 = "ihhmap-left2"
  val REDIS_HM_RIGHT_IHH1 = "ihhmap-right1"
  val REDIS_HM_RIGHT_IHH2 = "ihhmap-right2"

  var ehhMap = new HashMap[String, String]()

  var ihhMap_Left1 = new HashMap[String, String]
  var ihhMap_Right1 = new HashMap[String, String]

  var ihhMap_Left2 = new HashMap[String, String]
  var ihhMap_Right2 = new HashMap[String, String]

  var allMemoryCacheMap = Map[String, HashMap[String, String]](
    REDIS_HM_EHH -> ehhMap,
    REDIS_HM_LEFT_IHH1 -> ihhMap_Left1,
    REDIS_HM_LEFT_IHH2 -> ihhMap_Left2,
    REDIS_HM_RIGHT_IHH1 -> ihhMap_Right1,
    REDIS_HM_RIGHT_IHH2 -> ihhMap_Right2)

  /**
   * 计算ehh值，首先reduceByKey，然后做乘法
   */
  def calculateEHH(start: Int, end: Int): Double = {
    val retrieveehh = findMemoryCache(REDIS_HM_EHH, start, end)
    if (retrieveehh >= 0) return (retrieveehh)

    // 将数据集切分区间[start, end], 然后统计每段字符串出现的频率
    val reduceCount = pedMapList.map(x => {
      val strLength = x._2._2.length
      val newEnd = if (end + 1 > strLength) strLength else end + 1
      (new String(x._2._2.substring(start.toInt, newEnd.toInt)), 1)
    }).filter(x => !x._1.equals("")).groupBy(x => x._1).map(x => x._2.size)

    val reduceCountList = reduceCount.toList
    val reduceCountSize = reduceCountList.size

    var diff = 0

    for (i <- 0 until reduceCountSize - 1) {
      diff = diff + reduceCountList(i) * reduceCountList.slice(i + 1, reduceCountSize).reduce(_ + _)
    }

    (1 - (diff * 2).toDouble / (pedCount * (pedCount - 1)).toDouble)
  }

  
  /**
   * 折半查找位点遗传距离左右边界（the first index ehh < threshold)
   */
  def findCutOff(core: Int, threshold: Double, right: Boolean, stepSize: Int): Int = {

    val corePos = snpMap.get(core).get

    var index = core
    var targetIndex = 0
    var isFinished = false
    var stepSizeTmp = stepSize

    while (index >= 0 && index < snpCount && !isFinished) {
      var olderIndex = index
      right match {
        case true => {
          if (index + stepSizeTmp < snpCount) {
            index = index + stepSizeTmp
          } else {
            index = index + 1
          }
        }
        case false => {
          if (index - stepSizeTmp >= 0) {
            index = index - stepSizeTmp
          } else {
            index = index - 1
          }
        }
      }

      if (index < 0) return 0
      if (index >= snpCount) return snpCount - 1

      var indexPos = snpMap.get(index).get

      if (math.abs(indexPos - corePos) > 4000000) {
        targetIndex = index
        isFinished = true
      }

      if (!isFinished) {
        val bigger = if (core >= index) core else index
        val smaller = if (core >= index) index else core
        val indexEhh = calculateEHH(smaller, bigger)
        insertMemoryCache(REDIS_HM_EHH, smaller, bigger, indexEhh)

        if (indexEhh < threshold) {
          if (stepSizeTmp == 1) {
            targetIndex = index
            isFinished = true
          } else {
            index = olderIndex
            stepSizeTmp = stepSizeTmp / 2
          }
        }
      }
    }

    if (index == snpCount - 1) index else targetIndex
  }

  
  /**
   * IHH积分
   */
  def integrateIHH(core: Int, stop: Int, right: Boolean): Double = {
    var total = 0.0
    if (right) {
      // 向Index右侧积分
      // core________________stop
      calculateIHHRight(core, stop)

      total = findMemoryCache(REDIS_HM_RIGHT_IHH1, core, core) // 这个值是在calculateIHHRight(index, right)中计算的，第一个只有右侧IHH

      for (i <- core + 1 until stop) {
        total = total + findMemoryCache(REDIS_HM_LEFT_IHH1, core, i) + findMemoryCache(REDIS_HM_RIGHT_IHH1, core, i)
      }

      total = total + findMemoryCache(REDIS_HM_LEFT_IHH1, core, stop) // 这个值是在calculateIHHRight(index, right)中计算的，最后一个只有左侧IHH
    } else {
      // 向Index左侧积分
      // stop________________core
      calculateIHHLeft(core, stop)

      if (core > 0)
        total = findMemoryCache(REDIS_HM_LEFT_IHH2, core, core) // 这个值是在calculateIHHLeft(index, right)中计算的，最后一个只有左侧IHH

      for (i <- core - 1 until stop by -1) {
        total = total + findMemoryCache(REDIS_HM_RIGHT_IHH2,i, core) + findMemoryCache(REDIS_HM_LEFT_IHH2, i, core)
      }

      if (core > 0)
        total = total + findMemoryCache(REDIS_HM_RIGHT_IHH2,stop, core)
    }
    total
  }

  // 回调函数，对象销毁时候调用
  lazy val hook = new Thread {
    override def run = {
      // 清除缓存
      cleanUpCache
      // 清除资源
      cleanUpResource
    }
  }
  sys.addShutdownHook(hook.run)

  // 对象清理时候调用
  override protected def finalize() {
    // 清除缓存
    cleanUpCache
    // 清除资源
    cleanUpResource
  }

  /**
   * 清理工作，清除缓存
   */
  def cleanUpCache = {
    deleteCachedIHH
    deleteCachedEHH
  }

  /**
   * 清理工作，清除资源，如Redis数据库连接
   */
  def cleanUpResource: Unit = {}

  /**
   * 计算Index右侧IHH，缓存之
   * core__________stop
   */
  private def calculateIHHRight(core: Int, stop: Int): Unit = {
    if (findMemoryCache(REDIS_HM_LEFT_IHH1, core, stop) >= 0) return

    for (i <- core to stop) {
      if (findMemoryCache(REDIS_HM_LEFT_IHH1, core, i) >= 0) return

      val dist_left = if (i > 0)
        snpMap.get(i).get - snpMap.get(i - 1).get
      else 0

      val dist_right = if (i < snpCount - 2)
        snpMap.get(i + 1).get - snpMap.get(i).get
      else 0

      // 从Redis缓存中取EHH值，如果没有取到，说明ehh值为0.0
      val cachedEhh = findMemoryCache(REDIS_HM_EHH, core, i)
      val currentEhh = if (cachedEhh < 0) 0.0 else cachedEhh

      insertMemoryCache(REDIS_HM_LEFT_IHH1, core, i, currentEhh * dist_left / 2000000.0)
      insertMemoryCache(REDIS_HM_RIGHT_IHH1,core, i, currentEhh * dist_right / 2000000.0)
    }
  }

  /**
   * 计算Index左侧IHH，缓存之
   * stop__________core
   */
  private def calculateIHHLeft(core: Int, stop: Int): Unit = {
    if (findMemoryCache(REDIS_HM_LEFT_IHH2, stop, core) >= 0) return

    for (i <- core to stop by -1) {
      if (findMemoryCache(REDIS_HM_LEFT_IHH2, i, core) >= 0) return

      val dist_left = if (i > 0)
        snpMap.get(i).get - snpMap.get(i - 1).get
      else 0

      val dist_right = if (i < snpCount - 2)
        snpMap.get(i + 1).get - snpMap.get(i).get
      else 0

      // 从Redis缓存中取EHH值，如果没有取到，说明ehh值为0.0
      val cachedEhh = findMemoryCache(REDIS_HM_EHH, i, core)
      val currentEhh = if (cachedEhh < 0) 0.0 else cachedEhh

      insertMemoryCache(REDIS_HM_LEFT_IHH2, i, core, currentEhh * dist_left / 2000000.0)
      insertMemoryCache(REDIS_HM_RIGHT_IHH2, i, core, currentEhh * dist_right / 2000000.0)
    }
  }
  
  private def deleteCachedIHH() = {
    allMemoryCacheMap.get(REDIS_HM_LEFT_IHH1).get.clear()
    allMemoryCacheMap.get(REDIS_HM_LEFT_IHH2).get.clear()
    allMemoryCacheMap.get(REDIS_HM_RIGHT_IHH1).get.clear()
    allMemoryCacheMap.get(REDIS_HM_RIGHT_IHH2).get.clear()
  }

  private def deleteCachedEHH() = {
    allMemoryCacheMap.get(REDIS_HM_EHH).get.clear()
  }

  /**
   * 缓存到本地
   */
  private def insertMemoryCache(hmName: String, start: Int, end: Int, value: Double) = {
    val key = start + "-" + end
    var currentCacheMap = allMemoryCacheMap.get(hmName).get
    currentCacheMap.put(key, value.toString())
  }

  /**
   * 查找本地缓存
   */
  private def findMemoryCache(hmName: String, start: Int, end: Int): Double = {
    val key = start + "-" + end
    val currentCacheMap = allMemoryCacheMap.get(hmName).get
    val mapCache = currentCacheMap.getOrElse(key, "")
    if (mapCache == null || mapCache.equals("")) {
      -2.0
    } else mapCache.toDouble
  }
}