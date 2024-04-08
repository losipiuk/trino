/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.MoreSizeOfMain;
import io.trino.spi.MoreSizeOf;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.OPTIONAL_INT_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.OPTIONAL_LONG_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class PageBufferClientStatus
{
    private static final long INSTANCE_SIZE = instanceSize(PageBufferClientStatus.class);

    private final URI uri;
    private final String state;
    private final DateTime lastUpdate;
    private final long rowsReceived;
    private final int pagesReceived;
    // use optional to keep the output size down, since this renders for every destination
    private final OptionalLong rowsRejected;
    private final OptionalInt pagesRejected;
    private final int requestsScheduled;
    private final int requestsCompleted;
    private final int requestsFailed;
    private final int requestsSucceeded;
    private final String httpRequestState;

    @JsonCreator
    public PageBufferClientStatus(@JsonProperty("uri") URI uri,
            @JsonProperty("state") String state,
            @JsonProperty("lastUpdate") DateTime lastUpdate,
            @JsonProperty("rowsReceived") long rowsReceived,
            @JsonProperty("pagesReceived") int pagesReceived,
            @JsonProperty("rowsRejected") OptionalLong rowsRejected,
            @JsonProperty("pagesRejected") OptionalInt pagesRejected,
            @JsonProperty("requestsScheduled") int requestsScheduled,
            @JsonProperty("requestsCompleted") int requestsCompleted,
            @JsonProperty("requestsFailed") int requestsFailed,
            @JsonProperty("requestsSucceeded") int requestsSucceeded,
            @JsonProperty("httpRequestState") String httpRequestState)
    {
        this.uri = uri;
        this.state = state;
        this.lastUpdate = lastUpdate;
        this.rowsReceived = rowsReceived;
        this.pagesReceived = pagesReceived;
        this.rowsRejected = requireNonNull(rowsRejected, "rowsRejected is null");
        this.pagesRejected = requireNonNull(pagesRejected, "pagesRejected is null");
        this.requestsScheduled = requestsScheduled;
        this.requestsCompleted = requestsCompleted;
        this.requestsFailed = requestsFailed;
        this.requestsSucceeded = requestsSucceeded;
        this.httpRequestState = httpRequestState;
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public DateTime getLastUpdate()
    {
        return lastUpdate;
    }

    @JsonProperty
    public long getRowsReceived()
    {
        return rowsReceived;
    }

    @JsonProperty
    public int getPagesReceived()
    {
        return pagesReceived;
    }

    @JsonProperty
    public OptionalLong getRowsRejected()
    {
        return rowsRejected;
    }

    @JsonProperty
    public OptionalInt getPagesRejected()
    {
        return pagesRejected;
    }

    @JsonProperty
    public int getRequestsScheduled()
    {
        return requestsScheduled;
    }

    @JsonProperty
    public int getRequestsCompleted()
    {
        return requestsCompleted;
    }

    @JsonProperty
    public int getRequestsFailed()
    {
        return requestsFailed;
    }

    @JsonProperty
    public int getRequestsSucceeded()
    {
        return requestsSucceeded;
    }

    @JsonProperty
    public String getHttpRequestState()
    {
        return httpRequestState;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("uri", uri)
                .add("state", state)
                .add("lastUpdate", lastUpdate)
                .add("rowsReceived", rowsReceived)
                .add("pagesReceived", pagesReceived)
                .add("httpRequestState", httpRequestState)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + MoreSizeOf.sizeOf(uri)
                + estimatedSizeOf(state)
                + MoreSizeOfMain.sizeOf(lastUpdate)
                + OPTIONAL_LONG_INSTANCE_SIZE
                + OPTIONAL_INT_INSTANCE_SIZE
                + estimatedSizeOf(httpRequestState);
    }
}
