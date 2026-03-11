/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.materialize;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Metadata for the tagged interleaved row format used by merged indexes.
 *
 * <p>A merged index stores records from multiple source tables interleaved
 * by a shared sort key. Each record in the {@code Object[]} representation
 * has the following layout:
 *
 * <pre>
 *   [ domainTag0, keyVal0, domainTag1, keyVal1, ...,  'I', sourceId,  p0, p1, ..., pN ]
 *     ╰──────────── key fields with tags ──────╯  ╰─ index ID ─╯  ╰── payload ──╯
 * </pre>
 *
 * <ul>
 *   <li><b>Domain tags</b> (1 byte each): one per key field, identifying the
 *       domain (column type) of the following key value. For identical keys
 *       (all sources share the same key columns), tags are the same across
 *       sources. Domain 0 is reserved for the index identifier; key field
 *       domains are 1..K.</li>
 *   <li><b>Index identifier</b> (2 bytes): domain tag 0 ({@code 'I'}) + 1-byte
 *       source ID (0..sourceCount-1).</li>
 *   <li><b>Payload</b>: non-key columns, variable length per source.</li>
 * </ul>
 *
 * <p>This class tracks both <b>slot-based positions</b> (for {@code Object[]}
 * runtime access) and <b>byte widths</b> (for cost estimation / physical
 * B-tree layout). Byte widths are computed via
 * {@link RelMdSize#averageTypeValueSize(RelDataType)}.
 *
 * <p>Scoped to <em>identical keys</em> (all sources share the same key columns
 * at the same depth). Hierarchical keys are future work.
 *
 * @see MergedIndex
 */
public class TaggedRowSchema {

  /** Number of key fields (K). Same for all sources in identical-key mode. */
  public final int keyFieldCount;

  /**
   * Number of distinct domains: K key-field domains + 1 index domain.
   * Domain 0 = index identifier; domains 1..K = key fields.
   * Must be &lt; 255 to fit in a single byte.
   */
  public final int domainCount;

  /** Key field types, derived from source 0's row type. Length = K. */
  public final ImmutableList<RelDataType> keyFieldTypes;

  /** Byte width of each key field (from {@link RelMdSize}). Length = K. */
  public final ImmutableList<Double> keyFieldByteWidths;

  /** Number of sources in this merged index. Must be &lt; 255. */
  public final int sourceCount;

  /** Per-source row types. Indexed by source ID (0..sourceCount-1). */
  public final ImmutableList<RelDataType> sourceRowTypes;

  /**
   * Per-source key field indices: {@code keyFieldIndices[src][k]} is the
   * column index in source {@code src}'s row type for key field {@code k}.
   * Derived from each source's boundary collation.
   */
  public final ImmutableList<int[]> keyFieldIndices;

  /** Total payload bytes per source (sum of non-key column byte widths). */
  public final ImmutableList<Double> payloadByteWidths;

  /** Number of non-key columns per source. */
  public final ImmutableList<Integer> payloadFieldCounts;

  // -- Derived byte-width fields (for cost estimation) --------------------

  /** Key prefix byte width: K (tag bytes) + sum(keyFieldByteWidths). */
  public final double keyPrefixByteWidth;

  /** Index identifier byte width: always 2 (domain tag + source byte). */
  public static final int INDEX_ID_BYTE_WIDTH = 2;

  /** Per-source total record byte width: keyPrefix + indexId + payload. */
  public final ImmutableList<Double> totalRecordByteWidths;

  /**
   * Creates a TaggedRowSchema from a {@link MergedIndex}.
   *
   * @param mergedIndex the merged index whose sources define the schema
   */
  public TaggedRowSchema(MergedIndex mergedIndex) {
    final ImmutableList<Pipeline> sources = mergedIndex.getSources();
    this.sourceCount = sources.size();
    if (sourceCount >= 255) {
      throw new IllegalArgumentException(
          "Too many sources (" + sourceCount + "); must be < 255");
    }

    // Collect source row types and key field indices from boundary collations
    final ImmutableList.Builder<RelDataType> rowTypesBuilder =
        ImmutableList.builder();
    final ImmutableList.Builder<int[]> keyIndicesBuilder =
        ImmutableList.builder();

    for (Pipeline src : sources) {
      rowTypesBuilder.add(src.root.getRowType());
      final RelCollation bc = src.boundaryCollation;
      final List<RelFieldCollation> fields = bc.getFieldCollations();
      final int[] indices = new int[fields.size()];
      for (int k = 0; k < fields.size(); k++) {
        indices[k] = fields.get(k).getFieldIndex();
      }
      keyIndicesBuilder.add(indices);
    }
    this.sourceRowTypes = rowTypesBuilder.build();
    this.keyFieldIndices = keyIndicesBuilder.build();

    // Derive key field count from source 0's boundary collation
    this.keyFieldCount = keyFieldIndices.get(0).length;
    this.domainCount = keyFieldCount + 1; // domain 0 = index ID
    if (domainCount >= 255) {
      throw new IllegalArgumentException(
          "Too many domains (" + domainCount + "); must be < 255");
    }

    // Derive key field types and byte widths from source 0
    final ImmutableList.Builder<RelDataType> keyTypesBuilder =
        ImmutableList.builder();
    final ImmutableList.Builder<Double> keyBytesBuilder =
        ImmutableList.builder();
    double keyPrefixBytes = 0;
    final List<RelDataTypeField> src0Fields =
        sourceRowTypes.get(0).getFieldList();
    for (int k = 0; k < keyFieldCount; k++) {
      final RelDataType keyType =
          src0Fields.get(keyFieldIndices.get(0)[k]).getType();
      keyTypesBuilder.add(keyType);
      final Double byteWidth = averageTypeValueSize(keyType);
      final double w = byteWidth != null ? byteWidth : 8d; // default 8
      keyBytesBuilder.add(w);
      keyPrefixBytes += 1 + w; // 1-byte domain tag + key value
    }
    this.keyFieldTypes = keyTypesBuilder.build();
    this.keyFieldByteWidths = keyBytesBuilder.build();
    this.keyPrefixByteWidth = keyPrefixBytes;

    // Per-source payload metadata
    final ImmutableList.Builder<Double> payloadBytesBuilder =
        ImmutableList.builder();
    final ImmutableList.Builder<Integer> payloadCountsBuilder =
        ImmutableList.builder();
    final ImmutableList.Builder<Double> totalBytesBuilder =
        ImmutableList.builder();

    for (int s = 0; s < sourceCount; s++) {
      final List<RelDataTypeField> fields =
          sourceRowTypes.get(s).getFieldList();
      final Set<Integer> keySet = new LinkedHashSet<>();
      for (int idx : keyFieldIndices.get(s)) {
        keySet.add(idx);
      }
      double payloadBytes = 0;
      int payloadCount = 0;
      for (int f = 0; f < fields.size(); f++) {
        if (!keySet.contains(f)) {
          final Double bw =
              averageTypeValueSize(fields.get(f).getType());
          payloadBytes += bw != null ? bw : 8d;
          payloadCount++;
        }
      }
      payloadBytesBuilder.add(payloadBytes);
      payloadCountsBuilder.add(payloadCount);
      totalBytesBuilder.add(keyPrefixByteWidth + INDEX_ID_BYTE_WIDTH
          + payloadBytes);
    }
    this.payloadByteWidths = payloadBytesBuilder.build();
    this.payloadFieldCounts = payloadCountsBuilder.build();
    this.totalRecordByteWidths = totalBytesBuilder.build();
  }

  // -- Byte-size queries (cost estimation) --------------------------------

  /** Total record byte width for the given source. */
  public double totalRecordByteWidth(int sourceTag) {
    return totalRecordByteWidths.get(sourceTag);
  }

  /** Key prefix byte width (all domain tags + all key values). */
  public double keyPrefixByteWidth() {
    return keyPrefixByteWidth;
  }

  // -- Runtime helpers (Object[] slot-based access) -----------------------

  /**
   * Converts a source row ({@code Object[]} in table row-type order) into the
   * tagged interleaved format.
   *
   * <p>Uses this schema's {@code keyFieldIndices[sourceTag]} to determine
   * which source columns become key fields.
   *
   * @param sourceTag source index (0..sourceCount-1)
   * @param sourceRow row in the source table's column order
   * @return tagged row as {@code Object[]}
   */
  public Object[] toTaggedRow(int sourceTag, Object[] sourceRow) {
    final int slotCount = taggedRowSlotCount(sourceTag);
    final Object[] tagged = new Object[slotCount];
    final int[] keyIndices = keyFieldIndices.get(sourceTag);

    // Key fields with domain tags
    for (int k = 0; k < keyFieldCount; k++) {
      tagged[2 * k] = (byte) (k + 1);        // domain tag (1..K)
      tagged[2 * k + 1] = sourceRow[keyIndices[k]]; // key value
    }

    // Index identifier: domain 0 + source ID
    tagged[2 * keyFieldCount] = (byte) 0;           // index domain tag
    tagged[2 * keyFieldCount + 1] = (byte) sourceTag;

    // Payload: non-key columns in their original order
    final Set<Integer> keySet = new LinkedHashSet<>();
    for (int idx : keyIndices) {
      keySet.add(idx);
    }
    int payloadSlot = getPayloadStartSlot();
    for (int f = 0; f < sourceRow.length; f++) {
      if (!keySet.contains(f)) {
        tagged[payloadSlot++] = sourceRow[f];
      }
    }
    return tagged;
  }

  /**
   * Extracts the key value at position {@code keyIndex} from a tagged row.
   *
   * @param taggedRow the tagged row
   * @param keyIndex  key field index (0..keyFieldCount-1)
   * @return the key value object
   */
  public Object getKeyValue(Object[] taggedRow, int keyIndex) {
    return taggedRow[2 * keyIndex + 1];
  }

  /**
   * Extracts the source ID from a tagged row.
   *
   * @param taggedRow the tagged row
   * @return the source ID (0..sourceCount-1)
   */
  public byte getSourceId(Object[] taggedRow) {
    return (byte) taggedRow[2 * keyFieldCount + 1];
  }

  /**
   * Returns the slot index where payload fields begin in a tagged row.
   * This is {@code 2*K + 2} (K key tag-value pairs + 2 index-id slots).
   */
  public int getPayloadStartSlot() {
    return 2 * keyFieldCount + 2;
  }

  /**
   * Returns the total number of slots in a tagged row for the given source.
   * {@code 2*K + 2 + payloadFieldCounts[sourceTag]}.
   */
  public int taggedRowSlotCount(int sourceTag) {
    return 2 * keyFieldCount + 2 + payloadFieldCounts.get(sourceTag);
  }

  // -- Size estimation helper ---------------------------------------------

  /**
   * Estimates the average byte size of a value of the given type.
   * Delegates to the same logic as {@link RelMdSize#averageTypeValueSize}.
   */
  static @Nullable Double averageTypeValueSize(RelDataType type) {
    // Use a package-private accessor: instantiate RelMdSize via a trivial
    // subclass to work around the protected constructor.
    return SizeAccessor.INSTANCE.averageTypeValueSize(type);
  }

  /** Trivial subclass to access {@link RelMdSize}'s protected constructor. */
  private static class SizeAccessor extends RelMdSize {
    static final SizeAccessor INSTANCE = new SizeAccessor();
  }
}
