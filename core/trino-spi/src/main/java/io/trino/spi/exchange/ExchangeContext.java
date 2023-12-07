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
package io.trino.spi.exchange;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.ImplicitContextKeyed;
import io.trino.spi.QueryId;

import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class ExchangeContext
{
    // Use sth from io.opentelemetry.context to make maven dependency checker happy
    // and allow us to use opentelemetry-context in scope "compile".
    // We need scope compile because we use Span which inhertis from io.opentelemetry.context.ImplicitContextKeyed
    // which lives in opentelemetry-context.
    private static final ImplicitContextKeyed DUMMY = null;

    private final QueryId queryId;
    private final ExchangeId exchangeId;
    private final Span parentSpan;

    public ExchangeContext(QueryId queryId, ExchangeId exchangeId, Span parentSpan)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.parentSpan = requireNonNull(parentSpan, "parentSpan is null");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public ExchangeId getExchangeId()
    {
        return exchangeId;
    }

    public Span getParentSpan()
    {
        return parentSpan;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", ExchangeContext.class.getSimpleName() + "[", "]")
                .add("queryId=" + queryId)
                .add("exchangeId=" + exchangeId)
                .toString();
    }
}
