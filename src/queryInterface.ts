// src/queryInterface.ts
import { BigQuery } from "@google-cloud/bigquery";
import { BigQueryORM } from "./bigQueryORM";
import { DataType } from "./dataTypes";
import { dataTypeToSchemaField } from "./utils";

export class QueryInterface {
  QueryTypes = {
    SELECT: "SELECT",
    INSERT: "INSERT",
    UPDATE: "UPDATE",
    DELETE: "DELETE",
  };
  constructor(private orm: BigQueryORM) {}

  private dataTypeToString(type: DataType): string {
    this.orm.logger.info(
      "[QueryInterface:dataTypeToString] Converting data type to string",
      { type }
    );
    let base = "";
    if (type.type === "STRUCT") {
      base = `STRUCT<${Object.entries(type.fields || {})
        .map(([n, t]) => `\`${n}\` ${this.dataTypeToString(t)}`)
        .join(", ")}>`;
    } else if (["NUMERIC", "BIGNUMERIC", "DECIMAL"].includes(type.type)) {
      base = `${type.type}(${type.precision || 38}, ${type.scale || 9})`;
    } else {
      base = type.type;
    }

    if (type.mode === "REPEATED") {
      return `ARRAY<${base}>`;
    }
    return base;
  }

  async createTable(
    dataset: string,
    tableName: string,
    attributes: Record<string, DataType>,
    options: {
      partitionBy?: string;
      clusterBy?: string[];
      primaryKey?: string;
    } = {}
  ): Promise<void> {
    this.orm.logger.info("[QueryInterface:createTable] Starting createTable", {
      dataset,
      tableName,
      attributes: Object.keys(attributes),
      options,
    });
    if (this.orm.config.freeTierMode) {
      this.orm.logger.warn(
        "[QueryInterface:createTable] Free tier mode: Table creation counts toward 10GB storage limit."
      );
    }
    const ds = this.orm.bigquery.dataset(dataset);
    const [dsExists] = await ds.exists();
    if (!dsExists) {
      await ds.create();
      this.orm.logger.info(
        `[QueryInterface:createTable] Created dataset ${dataset}`
      );
    }
    const table = ds.table(tableName);
    const [tExists] = await table.exists();
    if (tExists) {
      this.orm.logger.info(
        `[QueryInterface:createTable] Table ${tableName} already exists in dataset ${dataset}, skipping creation`
      );
      return;
    }
    const schema = Object.entries(attributes).map(([name, type]) =>
      dataTypeToSchemaField(name, type)
    );
    const createOptions: any = { schema };
    if (options.partitionBy) {
      createOptions.timePartitioning = {
        type: "DAY",
        field: options.partitionBy,
      };
    }
    // Add clustering for primary key if provided (BigQuery's index equivalent)
    if (options.primaryKey) {
      createOptions.clustering = { fields: [options.primaryKey] };
      this.orm.logger.info(
        `[QueryInterface:createTable] Clustering table ${tableName} by primary key ${options.primaryKey} in dataset ${dataset}`
      );
    } else if (options.clusterBy) {
      createOptions.clustering = { fields: options.clusterBy };
    }
    await table.create(createOptions);
    this.orm.logger.info(
      `[QueryInterface:createTable] Created table ${tableName} in dataset ${dataset}`
    );
  }

  async dropTable(dataset: string, tableName: string): Promise<void> {
    this.orm.logger.info("[QueryInterface:dropTable] Starting dropTable", {
      dataset,
      tableName,
    });
    if (this.orm.config.freeTierMode) {
      this.orm.logger.warn(
        "[QueryInterface:dropTable] Free tier mode: Table deletion counts toward storage changes."
      );
    }
    const table = this.orm.bigquery.dataset(dataset).table(tableName);
    const [exists] = await table.exists();
    if (!exists) {
      this.orm.logger.info(
        `[QueryInterface:dropTable] Table ${tableName} does not exist in dataset ${dataset}, skipping deletion`
      );
      return;
    }
    await table.delete();
    this.orm.logger.info(
      `[QueryInterface:dropTable] Deleted table ${tableName} in dataset ${dataset}`
    );
  }

  async addColumn(
    dataset: string,
    tableName: string,
    columnName: string,
    type: DataType
  ): Promise<void> {
    this.orm.logger.info("[QueryInterface:addColumn] Starting addColumn", {
      dataset,
      tableName,
      columnName,
      type,
    });
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[QueryInterface:addColumn] Free tier mode: ADD COLUMN (DML) not allowed."
      );
      throw new Error("Free tier mode: ADD COLUMN (DML) not allowed.");
    }
    const dataTypeStr = this.dataTypeToString(type);
    const notNull = type.allowNull === false ? " NOT NULL" : "";
    const sql = `ALTER TABLE \`${this.orm.config.projectId}.${dataset}.${tableName}\` ADD COLUMN \`${columnName}\` ${dataTypeStr}${notNull}`;
    await this.orm.bigquery.query(sql);
    this.orm.logger.info(
      `[QueryInterface:addColumn] Added column ${columnName} to ${tableName} in dataset ${dataset}`
    );
  }

  async removeColumn(
    dataset: string,
    tableName: string,
    columnName: string
  ): Promise<void> {
    this.orm.logger.info(
      "[QueryInterface:removeColumn] Starting removeColumn",
      { dataset, tableName, columnName }
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[QueryInterface:removeColumn] Free tier mode: DROP COLUMN (DML) not allowed."
      );
      throw new Error("Free tier mode: DROP COLUMN (DML) not allowed.");
    }
    const sql = `ALTER TABLE \`${this.orm.config.projectId}.${dataset}.${tableName}\` DROP COLUMN IF EXISTS \`${columnName}\``;
    await this.orm.bigquery.query(sql);
    this.orm.logger.info(
      `[QueryInterface:removeColumn] Removed column ${columnName} from ${tableName} in dataset ${dataset}`
    );
  }

  async renameColumn(
    dataset: string,
    tableName: string,
    oldColumnName: string,
    newColumnName: string
  ): Promise<void> {
    this.orm.logger.info(
      "[QueryInterface:renameColumn] Starting renameColumn",
      { dataset, tableName, oldColumnName, newColumnName }
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[QueryInterface:renameColumn] Free tier mode: RENAME COLUMN (DML) not allowed."
      );
      throw new Error("Free tier mode: RENAME COLUMN (DML) not allowed.");
    }
    const sql = `ALTER TABLE \`${this.orm.config.projectId}.${dataset}.${tableName}\` RENAME COLUMN \`${oldColumnName}\` TO \`${newColumnName}\``;
    await this.orm.bigquery.query(sql);
    this.orm.logger.info(
      `[QueryInterface:renameColumn] Renamed column ${oldColumnName} to ${newColumnName} in ${tableName} in dataset ${dataset}`
    );
  }

  async changeColumn(
    dataset: string,
    tableName: string,
    columnName: string,
    type: DataType
  ): Promise<void> {
    this.orm.logger.info(
      "[QueryInterface:changeColumn] Starting changeColumn",
      { dataset, tableName, columnName, type }
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[QueryInterface:changeColumn] Free tier mode: ALTER COLUMN (DML) not allowed."
      );
      throw new Error("Free tier mode: ALTER COLUMN (DML) not allowed.");
    }
    const dataTypeStr = this.dataTypeToString(type);
    const sql = `ALTER TABLE \`${this.orm.config.projectId}.${dataset}.${tableName}\` ALTER COLUMN \`${columnName}\` SET DATA TYPE ${dataTypeStr}`;
    await this.orm.bigquery.query(sql);
    this.orm.logger.info(
      `[QueryInterface:changeColumn] Changed column ${columnName} type in ${tableName} in dataset ${dataset}`
    );
  }

  async addPartition(
    dataset: string,
    tableName: string,
    partitionBy: string
  ): Promise<void> {
    this.orm.logger.info(
      "[QueryInterface:addPartition] Starting addPartition",
      { dataset, tableName, partitionBy }
    );
    this.orm.logger.warn(
      "[QueryInterface:addPartition] Partitioning requires table recreation."
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[QueryInterface:addPartition] Free tier mode: Partition changes not supported."
      );
      throw new Error("Free tier mode: Partition changes not supported.");
    }
    // Note: BigQuery requires table recreation for partitioning changes
    this.orm.logger.info(
      "[QueryInterface:addPartition] Partitioning not directly supported; manual table recreation required."
    );
  }

  async addClustering(
    dataset: string,
    tableName: string,
    clusterBy: string[]
  ): Promise<void> {
    this.orm.logger.info(
      "[QueryInterface:addClustering] Starting addClustering",
      { dataset, tableName, clusterBy }
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.warn(
        "[QueryInterface:addClustering] Free tier mode: Clustering may incur query costs."
      );
    }
    const sql = `ALTER TABLE \`${
      this.orm.config.projectId
    }.${dataset}.${tableName}\` SET OPTIONS (clustering_fields = '${JSON.stringify(
      clusterBy
    )}')`;
    await this.orm.bigquery.query(sql);
    this.orm.logger.info(
      `[QueryInterface:addClustering] Added clustering to ${tableName} in dataset ${dataset}`
    );
  }

  async query(dataset: string, sql: string, params?: any): Promise<any> {
    this.orm.logger.info("[QueryInterface:query] Starting query execution", {
      dataset,
      sql,
      params,
    });
    if (
      this.orm.config.freeTierMode &&
      sql.trim().toUpperCase().startsWith("INSERT")
    ) {
      this.orm.logger.error(
        "[QueryInterface:query] Free tier mode: INSERT queries not allowed."
      );
      throw new Error("Free tier mode: INSERT queries not allowed.");
    }
    const result = await this.orm.bigquery.query({ query: sql, params });
    this.orm.logger.info(
      `[QueryInterface:query] Executed query successfully for dataset ${dataset}`
    );
    return result;
  }
}
