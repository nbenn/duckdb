#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetRowCount
#' @usage NULL
dbGetRowCount__duckdb_result_arrow <- function(res, ...) {
  dbGetRowCount_impl(res)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbGetRowCount", "duckdb_result_arrow", dbGetRowCount__duckdb_result_arrow)
