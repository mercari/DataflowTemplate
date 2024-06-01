package com.mercari.solution.util.pipeline.mutation;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.converter.MutationToTableRowConverter;
import com.mercari.solution.util.converter.RecordToTableRowConverter;
import com.mercari.solution.util.converter.RowToTableRowConverter;
import com.mercari.solution.util.converter.StructToTableRowConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutation;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class UnifiedMutation implements Serializable {

    private final String table;
    private final Long commitTimestampMicros;
    private final Integer sequence;

    private final MutationOp op;

    private final DataType type;

    private final Object value;


    public UnifiedMutation(
            final DataType type,
            final MutationOp op,
            final String table,
            final Long commitTimestampMicros,
            final Integer sequence,
            final Object value) {

        this.type = type;
        this.op = op;
        this.table = Optional.ofNullable(table).orElse("");
        this.commitTimestampMicros = commitTimestampMicros;
        this.sequence = Optional.ofNullable(sequence).orElse(-1);
        this.value = value;
    }

    public static UnifiedMutation of(Mutation mutation, String table, long commitTimestampMicros, final Integer sequence) {
        final MutationOp op = switch (mutation.getOperation()) {
            case INSERT -> MutationOp.INSERT;
            case UPDATE -> MutationOp.UPDATE;
            case REPLACE -> MutationOp.REPLACE;
            case INSERT_OR_UPDATE -> MutationOp.UPSERT;
            case DELETE -> MutationOp.DELETE;
        };
        return new UnifiedMutation(DataType.MUTATION, op, table, commitTimestampMicros, sequence, mutation);
    }

    public static UnifiedMutation copy(final UnifiedMutation unifiedMutation, long commitTimestampMicros) {
        return new UnifiedMutation(unifiedMutation.getType(), unifiedMutation.getOp(), unifiedMutation.getTable(), commitTimestampMicros, unifiedMutation.getSequence(), unifiedMutation.getValue());
    }

    public String getTable() {
        return table;
    }

    public Long getCommitTimestampMicros() {
        return commitTimestampMicros;
    }

    public Integer getSequence() {
        return sequence;
    }

    public MutationOp getOp() {
        return op;
    }

    public DataType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public Mutation getSpannerMutation() {
        if(!DataType.MUTATION.equals(this.type)) {
            throw new IllegalArgumentException("Not spanner mutation for type: " + this.type);
        }
        return (Mutation) value;
    }

    public RowMutation toRowMutation(final List<String> primaryKeyFields) {
        final TableRow tableRow = toTableRow(primaryKeyFields);
        final RowMutationInformation rowMutationInformation = toRowMutationInformation();
        return RowMutation.of(tableRow, rowMutationInformation);
    }

    public RowMutationInformation toRowMutationInformation() {
        final RowMutationInformation.MutationType mutationType = switch (this.op) {
            case INSERT, UPDATE, UPSERT -> RowMutationInformation.MutationType.UPSERT;
            case DELETE -> RowMutationInformation.MutationType.DELETE;
            default -> throw new IllegalArgumentException();
        };
        return RowMutationInformation.of(mutationType, this.commitTimestampMicros);
    }

    public TableRow toTableRow(final List<String> primaryKeyFields) {
        return switch (this.type) {
            case ROW -> RowToTableRowConverter.convert((Row) this.value);
            case AVRO -> RecordToTableRowConverter.convert((GenericRecord) this.value);
            case STRUCT -> StructToTableRowConverter.convert((Struct) this.value);
            case MUTATION -> MutationToTableRowConverter.convert((Mutation) this.value, primaryKeyFields);
            default -> throw new IllegalArgumentException();
        };
    }

}
