#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbSendQueryArrow
#' @inheritParams DBI::dbBind
#' @inheritParams duckdb_fetch_arrow
#' @usage NULL
dbSendQueryArrow__duckdb_connection_character <- function(conn, statement, params = NULL, ..., chunk_size = 1000000) {
  if (conn@debug) {
    message("Q ", statement)
  }
  statement <- enc2utf8(statement)
  stmt_lst <- rapi_prepare(conn@conn_ref, statement)

  res <- duckdb_result_arrow(
    connection = conn,
    stmt_lst = stmt_lst,
    chunk_size = chunk_size
  )
  if (length(params) > 0) {
    dbBind(res, params)
  }
  return(res)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbSendQueryArrow", c("duckdb_connection", "character"), dbSendQueryArrow__duckdb_connection_character)
