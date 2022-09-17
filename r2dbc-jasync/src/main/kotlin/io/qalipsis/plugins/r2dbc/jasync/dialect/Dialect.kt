/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.r2dbc.jasync.dialect

import com.github.jasync.sql.db.ConnectionPoolConfiguration
import com.github.jasync.sql.db.pool.ConnectionPool
import org.apache.calcite.avatica.util.Quoting

/**
 * Definition of database dialect to support different configurations for different vendors.
 *
 * @author Eric Jessé
 */
internal interface Dialect {

    /**
     * Configuration of the character to quote the tables or column names and avoid them
     * being interpreted as keywords.
     */
    val quotingConfig: Quoting

    /**
     * Connection builder for the underlying database.
     */
    val connectionBuilder: (ConnectionPoolConfiguration) -> ConnectionPool<*>

    /**
     * Adds the quote characters of the dialect around [tableOrColumnName].
     */
    fun quote(tableOrColumnName: String) = "${quotingConfig.string}$tableOrColumnName${quotingConfig.string}"
}
