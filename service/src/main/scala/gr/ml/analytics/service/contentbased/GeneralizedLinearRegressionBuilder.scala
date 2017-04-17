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

import gr.ml.analytics.util_old.SparkUtil
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.GeneralizedLinearRegression

// TODO this one fails - assertion failed: lapack.dppsv returned 7.
object GeneralizedLinearRegressionBuilder {

  // TODO path is NOT actually used...
  def build(path: String): Pipeline = {
    val spark = SparkUtil.sparkSession()

    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)

    val pipeline = new Pipeline()
      .setStages(Array(glr))

    spark.stop()

    pipeline
  }
}