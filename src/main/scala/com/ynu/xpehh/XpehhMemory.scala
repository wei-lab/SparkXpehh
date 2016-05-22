/*
 * XpehhMemory.scala for SparkXpehh
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

package com.ynu.xpehh

import scala.collection.mutable.HashMap
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import com.ynu.entity.XpehhEntity
import com.ynu.algorithm.AlgorithmMemory
import com.ynu.common.CommonUtility

/**
 * @author liucc
 * 
 * 完成所有xp-ehh计算：
 * 
 * 所有数据存在内存中
 * 将EHH缓存到内存中
 */
object XpehhMemory {
  def main(args: Array[String]): Unit = {
    /**
     * -h     <string>   ped file path
     * -m     <string>   map file path
     * -p     <string>   pop file path
     * -r     <string>   result file path
     *
     * -s     <int>      stepsize(default 16)
     * -t     <float>    threshold(default 0.05)
     * -ps    <int>      partition size(default 138)
     * -a     <int>      accuracy  size(default 10)
     *
     */
    if (args.length < 5) {
      System.err.println("Usage -h pedFilePath -m mapFilePath -p popFilePath -r resultFilePath -c cutoffFilePath")
      System.exit(1)
    }

    // parase the params
    val pedFilePath = if (args.indexOf("-h") > -1) args(args.indexOf("-h") + 1).trim else ""
    val snpFilePath = if (args.indexOf("-m") > -1) args(args.indexOf("-m") + 1).trim else ""
    val popFilePath = if (args.indexOf("-p") > -1) args(args.indexOf("-p") + 1).trim else ""
    val resultFilePath = if (args.indexOf("-r") > -1) args(args.indexOf("-r") + 1).trim else ""

    if (pedFilePath.equals("") || snpFilePath.equals("") || popFilePath.equals("") || resultFilePath.equals("")) {
      System.err.println("Usage -h pedFilePath -m mapFilePath -p popFilePath -r resultFilePath -c cutoffFilePath")
      System.exit(1)
    }

    val STEP_SIZE = {
      if (args.indexOf("-s") > -1) {
        val inputValue = args(args.indexOf("-s") + 1).toInt
        if (inputValue < 0) CommonUtility.DEFAULT_STEP_SIZE else inputValue
      } else CommonUtility.DEFAULT_STEP_SIZE
    }

    val THRESHOLD = {
      if (args.indexOf("-t") > -1) {
        val inputValue = args(args.indexOf("-s") + 1).toInt
        if (inputValue < 0 || inputValue > 1) CommonUtility.DEFAULT_THRESHOLD else inputValue
      } else CommonUtility.DEFAULT_THRESHOLD
    }

    val PARTITIONSIZE = {
      if (args.indexOf("-ps") > -1) {
        val inputValue = args(args.indexOf("-ps") + 1).toInt
        if (inputValue < 0) CommonUtility.DEFAULT_PARTITIONSIZE else inputValue
      } else CommonUtility.DEFAULT_PARTITIONSIZE
    }

    val ACCURACY = {
      if (args.indexOf("-a") > -1) {
        val inputValue = args(args.indexOf("-a") + 1).toInt
        if (inputValue < 0) CommonUtility.DEFAULT_ACCURACY else inputValue
      } else CommonUtility.DEFAULT_ACCURACY
    }

    val conf = new SparkConf()
    conf.set("spark.default.parallelism", PARTITIONSIZE.toString())
      .set("spark.akka.frameSize", "1024")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.shuffle.memoryFraction", "0.3")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") ///.setJars(jars)
      .set("spark.rdd.compress", "true")
      .set("spark.kryoserializer.buffer.mb", "256")

    val sparkContext = new SparkContext(conf)

    // 如果输出目录存在，删除
    val output = new org.apache.hadoop.fs.Path(resultFilePath);
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI("hdfs://hadoop-cluster"), new org.apache.hadoop.conf.Configuration())

    // 删除输出目录  
    if (hdfs.exists(output)) hdfs.delete(output, true)

    // 每个样本的变异字符串
    // <sample2952, ATGCCCGGATT.....>
    // 其中每个ATGC的长度，或是744426（3条）或是744425
    val pedFileRDD = sparkContext.textFile(pedFilePath).map(singleStr => {
      val sampleName = singleStr.substring(0, singleStr.indexOf("\t"))
      val samplePrefix = sampleName + sampleName + "0000"

      val sampleStr = singleStr.replace("\t", "").substring(samplePrefix.length())

      (sampleName, sampleStr)
    })

    // <0, 710>
    val snpFileRDD = sparkContext.textFile(snpFilePath).zipWithIndex.map(stringWithIndex => {
      val splitArray = stringWithIndex._1.split("\t")
      (stringWithIndex._2.toInt, splitArray(3).toInt)
    })

    // <sample2952, 1>
    val popFileRDD = sparkContext.textFile(popFilePath).map(x => x.split(" ")).map(x => (x(0), x(1)))

    // <sample2952,1>分发到不同机器
    val popFileRDDMapBroadcast = sparkContext.broadcast(popFileRDD.toLocalIterator.toMap)
    // <1, 710>分发到不同的机器
    val snpFileRDDMapBroadcast = sparkContext.broadcast(snpFileRDD.toLocalIterator.toMap)

    /**
     * 将每条ATGC长串拆分成等长的两条，其余信息不变，directorPedOrgRDD数量翻倍（pedOrgRDD）
     * 一条变异基因数据
     * <sample2952, ATGCCCGGATT.....>
     * 拆分成两条
     * <sample2952_0, 1, ATGCCCGGATT.....>
     * <sample2952_1, 1, GCGAAGGACCG.....>
     * 372213:53    372212:47
     *
     */
    val pedCombineRDD = pedFileRDD.mapPartitions(pedPatation => {
      val popMap = popFileRDDMapBroadcast.value
      pedPatation.flatMap(onePed => {
        val splitArray = CommonUtility.dividedSingleStr(onePed._2)
        val oneSample = for (
          i <- 0 until splitArray.length
        ) yield {
          (onePed._1 + "_" + i, (popMap.get(onePed._1).get, splitArray(i)))
        }
        oneSample
      })
    })

    pedCombineRDD.cache()
    
    // <(sample2952_0, (1, ATGCCCGGATT.....)),(sample2952_1, (1, GCGAAGGACCG.....))>所有数据分发到不同的机器
    val pedCombineRDDListBroadcast = sparkContext.broadcast(pedCombineRDD.toLocalIterator.toList)

    // 种类0和种类1的RDD
    val pedPop0RDD = pedCombineRDD.filter(x => x._2._1.equals("0"))
    val pedPop1RDD = pedCombineRDD.filter(x => x._2._1.equals("1"))

    // <(sample2952, (1, ATGCCCGGATT.....))>所有数据分发到不同的机器
    // 种类1分发
    val pedPop0RDDListBroadcast = sparkContext.broadcast(pedPop0RDD.toLocalIterator.toList)
    // 种类2分发
    val pedPop1RDDListBroadcast = sparkContext.broadcast(pedPop1RDD.toLocalIterator.toList)
    
    // 计算cutoff值:(index,left-right)
    val cutoffRDD = snpFileRDD.repartition(PARTITIONSIZE).mapPartitions(snpMapPartition => {

      val snpMap = snpFileRDDMapBroadcast.value
      val pedCombineListBC = pedCombineRDDListBroadcast.value

      val pedCombineEhhpop = new AlgorithmMemory(snpMap, pedCombineListBC)

      snpMapPartition.map(x => {
        val index = x._1 // index索引
        val snpPos = x._2 // snp位置

        val left = pedCombineEhhpop.findCutOff(index, THRESHOLD, false, STEP_SIZE)
        val right = pedCombineEhhpop.findCutOff(index, THRESHOLD, true, STEP_SIZE)

        (index, left + "-" + right)
      })
    })

    /**
     * 计算sample的xpehh值
     * <sample0Index,sample0Map<index, ehh>,sample1Index,sample1Map<index, ehh>>
     */
    def calculateSparXpEHH(cutoffRDD: RDD[(Int, String)]): RDD[XpehhEntity] = {
      // 遍历所有snp
      cutoffRDD.mapPartitions(cutoffPartition => {

        val snpMap = snpFileRDDMapBroadcast.value

        val sample0PedMap = pedPop0RDDListBroadcast.value
        val sample1PedMap = pedPop1RDDListBroadcast.value

        val sample0PedAlgorithm = new AlgorithmMemory(snpMap, sample0PedMap)
        val sample1PedAlgorithm = new AlgorithmMemory(snpMap, sample1PedMap)

        cutoffPartition.map(x => {
          val index = x._1
          val cutoffPos = x._2.split("-")

          val left = cutoffPos(0).toInt
          val right = cutoffPos(1).toInt

          // -----计算sample ehh------------------------
          var isFinished = false

          var sample0EHHMap = new HashMap[String, String]()
          var sample1EHHMap = new HashMap[String, String]()

          for (i <- left to right if !isFinished) {
            val ehh = if (i < index) {
              sample0PedAlgorithm.calculateEHH(i, index)
            } else {
              sample0PedAlgorithm.calculateEHH(index, i)
            }

            isFinished = (ehh == 0.0)
            val ehhString = BigDecimal(ehh).setScale(ACCURACY, RoundingMode.HALF_UP).toString()
            sample0EHHMap.put(i.toString(), ehhString)
          }

          isFinished = false
          for (i <- left to right if !isFinished) {
            val ehh = if (i < index) {
              sample1PedAlgorithm.calculateEHH(i, index)
            } else {
              sample1PedAlgorithm.calculateEHH(index, i)
            }

            isFinished = (ehh == 0.0)
            val ehhString = BigDecimal(ehh).setScale(ACCURACY, RoundingMode.HALF_UP).toString()
            sample1EHHMap.put(i.toString(), ehhString)
          }

          // -----计算integrate ehh------------------------
          val snpPos = snpMap.get(index).get // snp位置   

          sample0PedAlgorithm.cacheSampleEhh(index, sample0EHHMap)
          val p0_left = sample0PedAlgorithm.integrateIHH(index, left, false)
          val p0_right = sample0PedAlgorithm.integrateIHH(index, right, true)
          val IA = BigDecimal(p0_left + p0_right).setScale(ACCURACY, RoundingMode.HALF_UP)

          sample1PedAlgorithm.cacheSampleEhh(index, sample1EHHMap)
          val p1_left = sample1PedAlgorithm.integrateIHH(index, left, false)
          val p1_right = sample1PedAlgorithm.integrateIHH(index, right, true)
          val IB = BigDecimal(p1_left + p1_right).setScale(ACCURACY, RoundingMode.HALF_UP)

          val ratio = (IA / IB)
          val logratio = BigDecimal(math.log(ratio.toDouble)).setScale(ACCURACY, RoundingMode.HALF_UP)

          // 清除缓存
          sample0PedAlgorithm.cleanUpCache
          sample1PedAlgorithm.cleanUpCache

          XpehhEntity(snpPos, IA, IB, logratio)
        })
      })
    }

    calculateSparXpEHH(cutoffRDD).sortBy(x => x.pos, true, 1).saveAsTextFile(resultFilePath)

    sparkContext.stop()
  }

}