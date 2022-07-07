package zio.query

import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * `QueryFailure` keeps track of details relevant to query failures.
 */
final case class QueryFailure(dataSource: DataSource[Nothing, Nothing], request: Request[Any, Any])
    extends Throwable(null, null, true, false) {
  override def getMessage: String =
    s"Data source ${dataSource.identifier} did not complete request ${request.toString}."
}
