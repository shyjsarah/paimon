/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.clone.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.clone.files.CloneFilesProcessFunction;
import org.apache.paimon.hive.clone.HiveCloneUtils;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** create target paimon table. */
public class CloneHiveSchemaFunction
        extends CloneFilesProcessFunction<Tuple2<Identifier, Identifier>, CloneSchemaInfo> {

    private static final long serialVersionUID = 1L;

    public CloneHiveSchemaFunction(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        super(sourceCatalogConfig, targetCatalogConfig);
    }

    @Override
    public void processElement(
            Tuple2<Identifier, Identifier> tuple,
            ProcessFunction<Tuple2<Identifier, Identifier>, CloneSchemaInfo>.Context context,
            Collector<CloneSchemaInfo> collector)
            throws Exception {
        String sourceType = sourceCatalogConfig.get(CatalogOptions.METASTORE.key());
        checkNotNull(sourceType);

        // create database if not exists
        Map<String, String> databaseOptions =
                HiveCloneUtils.getDatabaseOptions(hiveCatalog, tuple.f0.getDatabaseName());
        targetCatalog.createDatabase(tuple.f1.getDatabaseName(), true, databaseOptions);

        Schema schema = null;
        try {
            schema = HiveCloneUtils.hiveTableToPaimonSchema(hiveCatalog, tuple.f0);
        } catch (UnsupportedOperationException e) {
            LOG.warn("skip unsupported format table: " + tuple.f0.getFullName());
            return;
        }

        Map<String, String> options = schema.options();
        // only support Hive to unaware-bucket table now
        options.put(CoreOptions.BUCKET.key(), "-1");
        schema =
                new Schema(
                        schema.fields(),
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        options,
                        schema.comment());
        try {
            Table existedTable = targetCatalog.getTable(tuple.f1);

            checkState(
                    existedTable instanceof FileStoreTable,
                    String.format(
                            "existed paimon table '%s' is not a FileStoreTable, but a %s",
                            tuple.f1, existedTable.getClass().getName()));
            checkCompatible(schema, (FileStoreTable) existedTable);

            LOG.info("paimon table '{}' already exists, use it as target table.", tuple.f1);
        } catch (Catalog.TableNotExistException e) {
            LOG.info("create target paimon table '{}'.", tuple.f1);

            targetCatalog.createTable(tuple.f1, schema, false);
        }

        CloneSchemaInfo cloneSchemaInfo = new CloneSchemaInfo(tuple);
        collector.collect(cloneSchemaInfo);
    }

    private void checkCompatible(Schema sourceSchema, FileStoreTable existedTable) {
        Schema existedSchema = existedTable.schema().toSchema();

        // check primary keys
        checkState(
                existedSchema.primaryKeys().isEmpty(),
                "Can not clone data to existed paimon table which has primary keys. Existed paimon table is "
                        + existedTable.name());

        // check bucket
        checkState(
                existedTable.coreOptions().bucket() == -1,
                "Can not clone data to existed paimon table which bucket is not -1. Existed paimon table is "
                        + existedTable.name());

        // check format
        checkState(
                Objects.equals(
                        sourceSchema.options().get(FILE_FORMAT.key()),
                        existedTable.coreOptions().formatType()),
                "source table format is not compatible with existed paimon table format.");

        // check partition keys
        List<String> sourcePartitionFields = sourceSchema.partitionKeys();
        List<String> existedPartitionFields = existedSchema.partitionKeys();

        checkState(
                sourcePartitionFields.size() == existedPartitionFields.size()
                        && new HashSet<>(existedPartitionFields).containsAll(sourcePartitionFields),
                "source table partition keys is not compatible with existed paimon table partition keys.");

        // check all fields
        List<DataField> sourceFields = sourceSchema.fields();
        List<DataField> existedFields = existedSchema.fields();

        checkState(
                existedFields.size() >= sourceFields.size()
                        && new HashSet<>(existedPartitionFields).containsAll(sourcePartitionFields),
                "source table partition keys is not compatible with existed paimon table partition keys.");
    }
}
