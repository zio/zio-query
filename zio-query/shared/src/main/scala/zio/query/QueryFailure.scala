/*
 * Copyright 2019-2023 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.query

import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * `QueryFailure` keeps track of details relevant to query failures. It is the
 * defect a query dies with when a data source returns no result for a request
 * it received: `dataSource` is that data source and `request` is the request.
 * The message names both, and the exception has no stack trace.
 */
final case class QueryFailure(dataSource: DataSource[Nothing, Nothing], request: Request[_, _])
    extends Throwable(null, null, true, false) {
  override def getMessage: String =
    s"Data source ${dataSource.identifier} did not complete request ${request.toString}."
}
