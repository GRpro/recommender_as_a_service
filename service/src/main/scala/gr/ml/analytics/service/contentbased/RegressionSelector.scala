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

import gr.ml.analytics.Constants
import gr.ml.analytics.util.SparkUtil
import org.apache.spark.ml.{Estimator, Model, Pipeline}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.functions.col

import scala.collection.mutable

object RegressionSelector extends App with Constants{
    var rfPrecisions:mutable.MutableList[Double] = mutable.MutableList()
    for(userId <- Range(1,10)){
      val precision:Double = evaluatePrecisionForUser(userId, RandomForestEstimatorBuilder.build(userId))
      if(precision != 0.0)
        rfPrecisions += precision
    }
  val rfAvgPrecision = rfPrecisions.sum/rfPrecisions.size
  println("Random Forest average precision = " + rfAvgPrecision)

//    var lrPrecisions:mutable.MutableList[Double] = mutable.MutableList()
//    for(userId <- Range(1,10)){
//      val precision:Double = evaluatePrecisionForUser(userId, null)
//      if(precision != 0.0)
//        lrPrecisions += precision
//    }
//  val lrAvgPrecision =lrPrecisions.sum/lrPrecisions.size
//  println("Linear Regression average precision = " + lrAvgPrecision)


  def evaluatePrecisionForUser(userId: Int, estimator:Estimator[Model[_]]): Double ={
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    // Load and parse the data file, converting it to a DataFrame.
    val data = spark.read.format("libsvm")
      .load(String.format(ratingsWithFeaturesSVMPath, userId.toString))

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train model. This also runs the indexer.
    val model = estimator.fit(trainingData)

    // TODO
    // to call method model.transform we need Estimator[Model[_]]
    // but Pipeline thats returned by RFEB is Estimator[PipelineModel]
    // which is not a descendent of Estimator[Model[_]] - is it true for scala??



    // Make predictions.
    val predictions = model.transform(testData)

    val numberOfPositivelyRated = testData.filter($"label" >= 4.0).count().toInt
    val tp = predictions.orderBy(col("prediction").desc)
      .limit(numberOfPositivelyRated)
      .filter($"label">=4.0).count()

    val precision = tp.toDouble/numberOfPositivelyRated
    precision
  }
}
