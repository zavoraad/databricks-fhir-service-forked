package com.databricks.industry.solutions.fhirapi.queries

import java.sql.{Connection, PreparedStatement, Types}

/*
    This class is only used internally to piece together multiple queries
    for database santiation with JDBC Prepared Statements

    sqlTemplate placeholder = ?
 */
case class ParameterizedQuery(
    val sqlTemplate: String,
    val parameters: Option[Seq[Any]]
) {

  /** Appends another ParameterizedQuery: templates joined with a space,
    * parameters concatenated.
    */
  def append(other: ParameterizedQuery): ParameterizedQuery =
    ParameterizedQuery(
      sqlTemplate = (sqlTemplate.trim + " " + other.sqlTemplate.trim).trim,
      parameters = Some(
        parameters.getOrElse(Seq.empty) ++ other.parameters.getOrElse(Seq.empty)
      )
    )

  /** Same as append; allows q1 ++ q2 syntax. */
  def ++(other: ParameterizedQuery): ParameterizedQuery = append(other)

  /** Plain-text SQL with parameters substituted (for display/logging; not for
    * execution).
    */
  override def toString: String = {
    val params = parameters.getOrElse(Seq.empty)
    val sb = new StringBuilder
    var i = 0
    var pIdx = 0
    while (i < sqlTemplate.length) {
      if (
        pIdx < params.length && i < sqlTemplate.length && sqlTemplate.charAt(
          i
        ) == '?'
      ) {
        val value = params(pIdx)
        pIdx += 1
        value match {
          case null      => sb.append("NULL")
          case s: String =>
            sb.append("'").append(s.replace("'", "''")).append("'")
          case _ => sb.append(value.toString)
        }
        i += 1
      } else {
        sb.append(sqlTemplate.charAt(i))
        i += 1
      }
    }
    sb.toString
  }

  def asPreparedStatement(conn: Connection): PreparedStatement = {
    val ps = conn.prepareStatement(sqlTemplate)
    parameters.getOrElse(Seq.empty).zipWithIndex.foldLeft(ps) {
      case (stmt, (value, i)) =>
        val idx = i + 1
        value match {
          case null =>
            stmt.setNull(idx, Types.OTHER)
          case v: String =>
            stmt.setString(idx, v)
          case v: Int =>
            stmt.setInt(idx, v)
          case v: Long =>
            stmt.setLong(idx, v)
          case v: Boolean =>
            stmt.setBoolean(idx, v)
          case v: Short =>
            stmt.setShort(idx, v)
          case v: Byte =>
            stmt.setByte(idx, v)
          case v: Float =>
            stmt.setFloat(idx, v)
          case v: Double =>
            stmt.setDouble(idx, v)
          case v: java.lang.Number =>
            stmt.setObject(idx, v)
          case v: java.util.Date =>
            stmt.setTimestamp(idx, new java.sql.Timestamp(v.getTime))
          case v: java.time.Instant =>
            stmt.setTimestamp(idx, java.sql.Timestamp.from(v))
          case v: AnyRef =>
            stmt.setObject(idx, v)
          case v =>
            stmt.setObject(idx, v.asInstanceOf[AnyRef])
        }
        stmt
    }
  }
}
