/*
 * Copyright (c) 2023 DeltaStream, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.deltastream.flink.connector.snowflake.sink.internal;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava31.com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

/** Util class for providing wrappers for common Snowflake related static logic. */
public class SnowflakeInternalUtils {

    /**
     * Generate a name for ingest client or channel from given parts, skipping null or empty parts.
     *
     * @param prefix {@link java.lang.String} a nullable prefix
     * @param name {@link java.lang.String} a name
     * @param id {@link java.lang.Integer} an identifier number
     * @return {@link java.lang.String} concatenated non-empty and non-null parts, separated by "_"
     */
    public static String createClientOrChannelName(
            @Nullable final String prefix, final String name, @Nullable final Integer id) {
        Preconditions.checkState(
                StringUtils.isNotBlank(prefix) || StringUtils.isNotBlank(name),
                "One of prefix or name must be set for ingest client/channel name");
        return Joiner.on("_")
                .skipNulls()
                .join(
                        StringUtils.isBlank(prefix) ? null : prefix,
                        StringUtils.isEmpty(name) ? null : name,
                        id);
    }
}
