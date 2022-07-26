#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetRowsAffected
#' @usage NULL
dbGetRowsAffected__duckdb_result_arrow <- function(res, ...) {
  0
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbGetRowsAffected", "duckdb_result_arrow", dbGetRowsAffected__duckdb_result_arrow)
