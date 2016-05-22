/*
 * CommonUtility.scala for SparkXpehh
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

package com.ynu.common

/**
 * @author liucc
 * 
 * 公共属性方法
 */
object CommonUtility {
  // 折半查找默认移动步长
  val DEFAULT_STEP_SIZE = 16
  // EHH默认阈值
  val DEFAULT_THRESHOLD = 0.05
  // Spark分区数目
  val DEFAULT_PARTITIONSIZE = 138
  // EHH、IHH结果精度
  val DEFAULT_ACCURACY = 8

  // Redis中EHH对象前缀
  val REDIS_HM_EHH_PRE = "ped"

  /**
   * 将一个字符串分成两个
   */
  def dividedSingleStr(singleStr: String): List[String] = {
    val str1 = new StringBuffer
    val str2 = new StringBuffer
    for (i <- 0 until singleStr.length()) {
      if (i % 2 == 0) str1.append(singleStr(i)) else str2.append(singleStr(i))
    }

    List(str1.toString, str2.toString)
  }
}