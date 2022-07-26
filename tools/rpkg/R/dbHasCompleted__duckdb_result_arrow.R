#' @rdname duckdb_result-class
#' @inheritParams DBI::dbHasCompleted
#' @usage NULL
dbHasCompleted__duckdb_result_arrow <- function(res, ...) {
  exists("is_complete", envir = res@env) && isTRUE(res@env$is_complete)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbHasCompleted", "duckdb_result_arrow", dbHasCompleted__duckdb_result_arrow)
