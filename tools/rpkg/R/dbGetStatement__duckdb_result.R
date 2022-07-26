#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetStatement
#' @usage NULL
dbGetStatement__duckdb_result <- function(res, ...) {
  dbGetStatement_impl(res)
}

dbGetStatement_impl <- function(res) {
  if (!res@env$open) {
    stop("result has already been cleared")
  }
  res@stmt_lst$str
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbGetStatement", "duckdb_result", dbGetStatement__duckdb_result)
