#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetStatement
#' @usage NULL
dbGetStatement__duckdb_result_arrow <- function(res, ...) {
  dbGetStatement_impl(res)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbGetStatement", "duckdb_result_arrow", dbGetStatement__duckdb_result_arrow)
