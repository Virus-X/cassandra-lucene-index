package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.MsgPackMapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by vgoncharenko on 20.07.2015.
 */
public class MsgPackMapperBuilder extends MapperBuilder<MsgPackMapper> {
    @JsonProperty("indexed")
    private Boolean indexed;

    @JsonProperty("sorted")
    private Boolean sorted;

    @JsonProperty("case_sensitive")
    private Boolean caseSensitive;

    public MsgPackMapperBuilder indexed(Boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    public MsgPackMapperBuilder sorted(Boolean sorted) {
        this.sorted = sorted;
        return this;
    }

    public MsgPackMapperBuilder caseSensitive(Boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    @Override
    public MsgPackMapper build(String name) {
        return new MsgPackMapper(name, indexed, sorted, caseSensitive);
    }
}
