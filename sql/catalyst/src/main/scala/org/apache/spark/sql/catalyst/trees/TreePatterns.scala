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

package org.apache.spark.sql.catalyst.trees

// Enums for commonly encountered tree patterns in rewrite rules.
object TreePattern extends Enumeration  {
  type TreePattern = Value

  // Enum Ids start from 0.
  // Expression patterns (alphabetically ordered)
  val ATTRIBUTE_REFERENCE = Value(0)
  val DYNAMIC_PRUNING_SUBQUERY: Value = Value
  val EXISTS_SUBQUERY = Value
  val EXPRESSION_WITH_RANDOM_SEED = Value
  val IN: Value = Value
  val IN_SUBQUERY: Value = Value
  val INSET: Value = Value
  val LIST_SUBQUERY: Value = Value
  val LITERAL: Value = Value
  val NULL_LITERAL: Value = Value
  val PLAN_EXPRESSION: Value = Value
  val SCALAR_SUBQUERY: Value = Value
  val TRUE_OR_FALSE_LITERAL: Value = Value
  val WINDOW_EXPRESSION: Value = Value

  // Logical plan patterns (alphabetically ordered)
  val FILTER: Value = Value
  val INNER_LIKE_JOIN: Value = Value
  val JOIN: Value = Value
  val LEFT_SEMI_OR_ANTI_JOIN: Value = Value
  val NATURAL_LIKE_JOIN: Value = Value
  val OUTER_JOIN: Value = Value
}
