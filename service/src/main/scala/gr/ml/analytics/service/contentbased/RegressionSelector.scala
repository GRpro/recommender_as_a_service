/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ml.analytics.service.contentbased

import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.contentbased.RandomForestEstimatorBuilder.ratingsWithFeaturesSVMPath
import gr.ml.analytics.util.SparkUtil
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.col

import scala.collection.mutable

object RegressionSelector extends App with Constants {

  val rfAvgPrecision = getAveragePrecision(RandomForestEstimatorBuilder.build)
  val lrAvgPrecision = getAveragePrecision(LinearRegressionWithElasticNetBuilder.build)

  def getAveragePrecision(buildPipeline: String => Pipeline): Double = {
    var allPrecisions: mutable.MutableList[Double] = mutable.MutableList()
    for (userId <- Range(1, 10)) { // TODO unhardcode userId range, can use more functional style I guess
      val precision: Double = evaluatePrecisionForUser(userId, buildPipeline(mainSubDir))
      if (precision != 0.0 && !precision.isNaN)
        allPrecisions += precision
    }
    allPrecisions.sum / allPrecisions.size
  }

  def evaluatePrecisionForUser(userId: Int, pipeline: Pipeline): Double = {
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val data = spark.read.format("libsvm")
      .load(String.format(ratingsWithFeaturesSVMPath, userId.toString))

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    val numberOfPositivelyRated = testData.filter($"label" >= 4.0).count().toInt
    val tp = predictions.orderBy(col("rating").desc)
      .limit(numberOfPositivelyRated)
      .filter($"label" >= 4.0).count()

    val precision = tp.toDouble / numberOfPositivelyRated
    precision
  }
}
