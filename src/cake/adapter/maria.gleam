//// 🎂Cake 🦭MariaDB adapter which passes `PreparedStatement`s
//// to the `gmysql` library for execution.
////

import cake.{
  type CakeQuery, type PreparedStatement, type ReadQuery, type WriteQuery,
  CakeReadQuery, CakeWriteQuery,
}
import cake/dialect/maria_dialect
import cake/param.{
  type Param, BoolParam, FloatParam, IntParam, NullParam, StringParam,
}
import gleam/dynamic.{type DecodeError, type Dynamic}
import gleam/list
import gleam/option.{type Option}
import gmysql.{type Connection, type Error}

/// Connection to a 🦭MariaDB database.
///
/// This is a thin wrapper around the `gmysql` library's `Connection` type.
///
pub fn with_connection(
  host host: String,
  port port: Int,
  username username: Option(String),
  password password: Option(String),
  database database: String,
  callback callback: fn(Connection) -> a,
) -> a {
  let assert Ok(connection) =
    gmysql.Config(
      host:,
      port:,
      user: username,
      password:,
      database:,
      connection_mode: gmysql.Synchronous,
      connection_timeout: gmysql.Infinity,
      keep_alive: 999_999_999,
    )
    |> gmysql.connect

  let value = callback(connection)
  gmysql.disconnect(connection)

  value
}

/// Convert a Cake `ReadQuery` to a `PreparedStatement`.
///
pub fn read_query_to_prepared_statement(
  query query: ReadQuery,
) -> PreparedStatement {
  query |> maria_dialect.read_query_to_prepared_statement
}

/// Convert a Cake `WriteQuery` to a `PreparedStatement`.
///
pub fn write_query_to_prepared_statement(
  query query: WriteQuery(a),
) -> PreparedStatement {
  query |> maria_dialect.write_query_to_prepared_statement
}

pub fn run_read_query(
  query query: ReadQuery,
  decoder decoder: fn(Dynamic) -> Result(a, List(DecodeError)),
  db_connection db_connection: Connection,
) {
  let prepared_statement = query |> read_query_to_prepared_statement
  let sql_string = prepared_statement |> cake.get_sql
  let db_params =
    prepared_statement
    |> cake.get_params
    |> list.map(with: cake_param_to_client_param)

  sql_string
  |> gmysql.query(on: db_connection, with: db_params, expecting: decoder)
}

/// Run a Cake `WriteQuery` against an 🦭MariaDB database.
///
pub fn run_write_query(
  query query: WriteQuery(a),
  decoder decoder: fn(Dynamic) -> Result(a, List(DecodeError)),
  db_connection db_connection: Connection,
) -> Result(List(a), Error) {
  let prepared_statement = query |> write_query_to_prepared_statement
  let sql_string = prepared_statement |> cake.get_sql
  let db_params =
    prepared_statement
    |> cake.get_params
    |> list.map(with: cake_param_to_client_param)

  sql_string
  |> gmysql.query(on: db_connection, with: db_params, expecting: decoder)
}

/// Run a Cake `CakeQuery` against an 🦭MariaDB database.
///
/// This function is a wrapper around `run_read_query` and `run_write_query`.
///
pub fn run_query(
  query query: CakeQuery(a),
  decoder decoder: fn(Dynamic) -> Result(a, List(DecodeError)),
  db_connection db_connection: Connection,
) -> Result(List(a), Error) {
  case query {
    CakeReadQuery(read_query) ->
      read_query |> run_read_query(decoder, db_connection)
    CakeWriteQuery(write_query) ->
      write_query |> run_write_query(decoder, db_connection)
  }
}

/// Execute a raw SQL query against an 🦭MariaDB database.
///
pub fn execute_raw_sql(
  query_string query_string: String,
  db_connection db_connection: Connection,
) -> Result(Nil, Error) {
  query_string |> gmysql.exec(on: db_connection)
}

fn cake_param_to_client_param(param param: Param) {
  case param {
    BoolParam(param) -> gmysql.to_param(param)
    FloatParam(param) -> gmysql.to_param(param)
    IntParam(param) -> gmysql.to_param(param)
    StringParam(param) -> gmysql.to_param(param)
    NullParam -> gmysql.to_param(Nil)
  }
}
