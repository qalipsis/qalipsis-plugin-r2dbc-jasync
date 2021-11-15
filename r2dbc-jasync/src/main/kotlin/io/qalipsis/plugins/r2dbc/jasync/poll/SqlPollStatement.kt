package io.qalipsis.plugins.r2dbc.jasync.poll

/**
 * SQL statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Eric Jessé
 */
internal interface SqlPollStatement {

    /**
     * The boundary value used to select the next batch of data.
     */
    var tieBreaker: Any?

    /**
     * Provides the SQL prepared statement given the past executions and tie-breaker value.
     */
    val query: String

    /**
     * Provides the initial parameters.
     */
    val parameters: List<Any?>

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()
}