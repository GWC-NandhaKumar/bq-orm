// src/model.ts
import { BigQuery } from "@google-cloud/bigquery";
import { BigQueryORM } from "./bigQueryORM";
import { Op, Operator } from "./op";
import { DataType, DataTypes } from "./dataTypes";
import { buildWhereClause } from "./utils";
import * as crypto from "crypto";

export interface WhereOptions {
  [key: string]: any | { [key in Operator]?: any } | WhereOptions[];
}

export interface IncludeOptions {
  model: typeof Model;
  as?: string;
  where?: WhereOptions;
  required?: boolean;
  attributes?: string[];
}

export interface FindOptions {
  attributes?: string[];
  where?: WhereOptions;
  include?: IncludeOptions[];
  order?: [string, "ASC" | "DESC"][];
  group?: string[];
  limit?: number;
  offset?: number;
  raw?: boolean;
  distinct?: boolean;
}

export interface Association {
  type: "hasOne" | "hasMany" | "belongsTo" | "belongsToMany";
  target: typeof Model;
  foreignKey: string;
  otherKey?: string;
  as?: string;
  through?: typeof Model;
}

export interface FindAndCountAllResult {
  rows: any[];
  count: number;
}

export interface BulkCreateOptions {
  validate?: boolean;
  ignoreDuplicates?: boolean;
  returning?: boolean;
}

export interface UpdateOptions {
  where: WhereOptions;
  returning?: boolean;
  individualHooks?: boolean;
}

export interface DestroyOptions {
  where: WhereOptions;
  force?: boolean;
  cascade?: boolean;
}

export abstract class Model {
  static orm: BigQueryORM;
  static tableName: string;
  static primaryKey: string = "id";
  static attributes: Record<string, DataType>;
  static associations: Record<string, Association> = {};
  static associate?: (models: Record<string, typeof Model>) => void;

  static init(
    attributes: Record<string, DataType>,
    options: { orm: BigQueryORM; tableName?: string; primaryKey?: string }
  ) {
    this.orm = options.orm;
    this.orm.logger.info(
      `[Model:init] Starting initialization for model: ${this.name}`
    );
    this.attributes = attributes;
    this.tableName = options.tableName || this.name.toLowerCase();
    this.primaryKey =
      options.primaryKey ||
      Object.keys(attributes).find((key) => attributes[key].primaryKey) ||
      "id";
    this.orm.logger.info(
      `[Model:init] Initialization complete for model: ${this.name}`
    );
  }

  static belongsTo(
    target: typeof Model,
    options: { foreignKey?: string; as?: string } = {}
  ) {
    const foreignKey = options.foreignKey || `${target.name.toLowerCase()}Id`;
    const as = options.as || target.name.toLowerCase();
    this.associations[as] = { type: "belongsTo", target, foreignKey, as };
    if (!this.attributes[foreignKey]) {
      this.attributes[foreignKey] = DataTypes.INTEGER();
    }
    this.orm.logger.info(
      `[Model:belongsTo] Set up belongsTo ${this.name} -> ${target.name}`
    );
  }

  static hasOne(
    target: typeof Model,
    options: { foreignKey?: string; as?: string } = {}
  ) {
    const foreignKey = options.foreignKey || `${this.name.toLowerCase()}Id`;
    const as = options.as || target.name.toLowerCase();
    this.associations[as] = { type: "hasOne", target, foreignKey, as };
    this.orm.logger.info(
      `[Model:hasOne] Set up hasOne ${this.name} -> ${target.name}`
    );
  }

  static hasMany(
    target: typeof Model,
    options: { foreignKey?: string; as?: string } = {}
  ) {
    const foreignKey = options.foreignKey || `${this.name.toLowerCase()}Id`;
    const as = options.as || `${target.name.toLowerCase()}s`;
    this.associations[as] = { type: "hasMany", target, foreignKey, as };
    this.orm.logger.info(
      `[Model:hasMany] Set up hasMany ${this.name} -> ${target.name}`
    );
  }

  static belongsToMany(
    target: typeof Model,
    options: {
      through: typeof Model;
      foreignKey?: string;
      otherKey?: string;
      as?: string;
    }
  ) {
    const foreignKey = options.foreignKey || `${this.name.toLowerCase()}Id`;
    const otherKey = options.otherKey || `${target.name.toLowerCase()}Id`;
    const as = options.as || `${target.name.toLowerCase()}s`;
    this.associations[as] = {
      type: "belongsToMany",
      target,
      foreignKey,
      otherKey,
      through: options.through,
      as,
    };
    this.orm.logger.info(
      `[Model:belongsToMany] Set up belongsToMany ${this.name} -> ${target.name}`
    );
  }

  static async findAll(
    dataset: string,
    options: FindOptions = {}
  ): Promise<any[]> {
    const { sql, params } = this.buildSelectQuery(dataset, options);
    this.orm.logger.info(
      `[Model:findAll] Executing query for ${this.name} in dataset ${dataset}`,
      {
        sql,
        params,
      }
    );
    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    const result = options.raw
      ? rows
      : this.nestAssociations(rows, options.include || []);
    this.orm.logger.info(
      `[Model:findAll] Found ${result.length} records for ${this.name} in dataset ${dataset}`
    );
    return result;
  }

  static async findOne(
    dataset: string,
    options: FindOptions = {}
  ): Promise<any | null> {
    this.orm.logger.info(
      `[Model:findOne] Finding one record for ${this.name} in dataset ${dataset}`,
      { options }
    );
    const results = await this.findAll(dataset, { ...options, limit: 1 });
    const result = results[0] || null;
    this.orm.logger.info(
      `[Model:findOne] Found record: ${result ? "yes" : "no"} for ${
        this.name
      } in dataset ${dataset}`
    );
    return result;
  }

  static async findByPk(
    dataset: string,
    pk: any,
    options: FindOptions = {}
  ): Promise<any | null> {
    this.orm.logger.info(
      `[Model:findByPk] Finding by PK for ${this.name} in dataset ${dataset}`,
      {
        pk,
        options,
      }
    );
    return this.findOne(dataset, {
      ...options,
      where: { [this.primaryKey]: pk },
    });
  }

  static async findAndCountAll(
    dataset: string,
    options: FindOptions = {}
  ): Promise<FindAndCountAllResult> {
    this.orm.logger.info(
      `[Model:findAndCountAll] Finding and counting records for ${this.name} in dataset ${dataset}`,
      { options }
    );

    const mainAlias = this.tableName;
    const selectClause: string[] = [];
    const params: Record<string, any> = {};
    let sql = `FROM \`${dataset}.${this.tableName}\` AS \`${mainAlias}\``;
    const whereClauses: string[] = [];

    // Build main select clause
    const mainAttributes = options.attributes || Object.keys(this.attributes);
    for (const field of mainAttributes) {
      selectClause.push(
        `\`${mainAlias}\`.\`${field}\` AS \`${mainAlias}_${field}\``
      );
    }

    // Handle includes
    if (options.include) {
      for (const inc of options.include) {
        const as = inc.as || inc.model.tableName;
        const assoc = Object.values(this.associations).find(
          (a) => a.as === as && a.target === inc.model
        );
        if (!assoc)
          throw new Error(`Association not found for ${inc.model.name}`);

        const joinType = inc.required ? "INNER JOIN" : "LEFT OUTER JOIN";
        let joinOn: string;

        if (assoc.type === "belongsTo") {
          joinOn = `\`${mainAlias}\`.\`${assoc.foreignKey}\` = \`${as}\`.\`${inc.model.primaryKey}\``;
          sql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON ${joinOn}`;
        } else if (assoc.type === "hasOne" || assoc.type === "hasMany") {
          joinOn = `\`${mainAlias}\`.\`${this.primaryKey}\` = \`${as}\`.\`${assoc.foreignKey}\``;
          sql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON ${joinOn}`;
        } else if (assoc.type === "belongsToMany") {
          if (!assoc.through || !assoc.otherKey)
            throw new Error(
              "Through model and otherKey required for belongsToMany"
            );
          const throughAs = `${as}_through`;
          const throughTable = assoc.through.tableName;
          joinOn = `\`${mainAlias}\`.\`${assoc.foreignKey}\` = \`${throughAs}\`.\`${assoc.foreignKey}\``;
          sql += ` ${joinType} \`${dataset}.${throughTable}\` AS \`${throughAs}\` ON ${joinOn}`;
          joinOn = `\`${throughAs}\`.\`${assoc.otherKey}\` = \`${as}\`.\`${inc.model.primaryKey}\``;
          sql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON ${joinOn}`;
        }

        if (inc.where) {
          const { clause, params: incParams } = buildWhereClause(inc.where);
          const prefixedClause = clause.replace(/`([^`]+)`/g, `\`${as}\`.$1`);
          whereClauses.push(prefixedClause);
          Object.assign(params, incParams);
        }

        // Include attributes + primary key
        const incAttributes = inc.attributes
          ? Array.from(new Set([inc.model.primaryKey, ...inc.attributes]))
          : Object.keys(inc.model.attributes);

        for (const field of incAttributes) {
          selectClause.push(`\`${as}\`.\`${field}\` AS \`${as}_${field}\``);
        }
      }
    }

    // Main WHERE
    if (options.where) {
      const { clause, params: mParams } = buildWhereClause(options.where);
      if (clause) whereClauses.push(clause);
      Object.assign(params, mParams);
    }

    const whereClause = whereClauses.length
      ? ` WHERE ${whereClauses.join(" AND ")}`
      : "";
    sql += whereClause;

    // Count subquery
    const countSelect = options.distinct
      ? `COUNT(DISTINCT \`${mainAlias}\`.\`${this.primaryKey}\`)`
      : `COUNT(*)`;
    let countSql = `SELECT ${countSelect} AS total_count FROM \`${dataset}.${this.tableName}\` AS \`${mainAlias}\``;
    if (options.include) {
      for (const inc of options.include) {
        const as = inc.as || inc.model.tableName;
        const assoc = Object.values(this.associations).find(
          (a) => a.as === as && a.target === inc.model
        );
        if (!assoc) continue;
        const joinType = inc.required ? "INNER JOIN" : "LEFT OUTER JOIN";
        if (assoc.type === "belongsTo") {
          countSql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON \`${mainAlias}\`.\`${assoc.foreignKey}\` = \`${as}\`.\`${inc.model.primaryKey}\``;
        } else if (assoc.type === "hasOne" || assoc.type === "hasMany") {
          countSql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON \`${mainAlias}\`.\`${this.primaryKey}\` = \`${as}\`.\`${assoc.foreignKey}\``;
        } else if (assoc.type === "belongsToMany") {
          const throughAs = `${as}_through`;
          const throughTable = assoc.through?.tableName;
          countSql += ` ${joinType} \`${dataset}.${throughTable}\` AS \`${throughAs}\` ON \`${mainAlias}\`.\`${assoc.foreignKey}\` = \`${throughAs}\`.\`${assoc.foreignKey}\``;
          countSql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON \`${throughAs}\`.\`${assoc.otherKey}\` = \`${as}\`.\`${inc.model.primaryKey}\``;
        }
      }
    }
    if (whereClause) countSql += whereClause;

    // Final query using CTE
    const finalSql = `
    WITH count_query AS (${countSql}),
         data_query AS (
           SELECT ${options.distinct ? "DISTINCT" : ""} ${selectClause.join(
      ", "
    )}
           ${sql}
           ${
             options.order
               ? `ORDER BY ${options.order
                   .map(([f, d]) => `\`${mainAlias}\`.\`${f}\` ${d}`)
                   .join(", ")}`
               : ""
           }
           ${options.limit ? `LIMIT ${options.limit}` : ""}
           ${options.offset ? `OFFSET ${options.offset}` : ""}
         )
    SELECT data_query.*, (SELECT total_count FROM count_query) AS total_count
    FROM data_query
  `;

    this.orm.logger.info(`[Model:findAndCountAll] Final SQL for ${this.name}`, {
      finalSql,
      params,
    });

    const [rows] = await this.orm.bigquery.query({ query: finalSql, params });
    const resultRows = options.raw
      ? rows
      : this.nestAssociations(rows, options.include || []);
    const count = rows[0]?.total_count || 0;

    this.orm.logger.info(
      `[Model:findAndCountAll] Found ${resultRows.length} rows with total count ${count}`
    );
    return { rows: resultRows, count };
  }

  static async count(
    dataset: string,
    options: FindOptions = {}
  ): Promise<number> {
    this.orm.logger.info(
      `[Model:count] Counting records for ${this.name} in dataset ${dataset}`,
      {
        options,
      }
    );
    const select = `COUNT(DISTINCT \`${this.tableName}\`.\`${this.primaryKey}\`) AS count`;
    const { sql, params } = this.buildSelectQuery(dataset, options, select);
    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    const count = rows[0]?.count || 0;
    this.orm.logger.info(
      `[Model:count] Counted ${count} records for ${this.name} in dataset ${dataset}`
    );
    return count;
  }

  static async max(
    dataset: string,
    field: string,
    options: FindOptions = {}
  ): Promise<number | null> {
    this.orm.logger.info(
      `[Model:max] Getting max value for field ${field} in ${this.name} in dataset ${dataset}`,
      {
        field,
        options,
      }
    );
    const select = `MAX(\`${this.tableName}\`.\`${field}\`) AS max_value`;
    const { sql, params } = this.buildSelectQuery(dataset, options, select);
    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    const maxValue = rows[0]?.max_value || null;
    this.orm.logger.info(
      `[Model:max] Max value for ${field}: ${maxValue} in ${this.name} in dataset ${dataset}`
    );
    return maxValue;
  }

  static async min(
    dataset: string,
    field: string,
    options: FindOptions = {}
  ): Promise<number | null> {
    this.orm.logger.info(
      `[Model:min] Getting min value for field ${field} in ${this.name} in dataset ${dataset}`,
      {
        field,
        options,
      }
    );
    const select = `MIN(\`${this.tableName}\`.\`${field}\`) AS min_value`;
    const { sql, params } = this.buildSelectQuery(dataset, options, select);
    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    const minValue = rows[0]?.min_value || null;
    this.orm.logger.info(
      `[Model:min] Min value for ${field}: ${minValue} in ${this.name} in dataset ${dataset}`
    );
    return minValue;
  }

  static async sum(
    dataset: string,
    field: string,
    options: FindOptions = {}
  ): Promise<number> {
    this.orm.logger.info(
      `[Model:sum] Getting sum for field ${field} in ${this.name} in dataset ${dataset}`,
      {
        field,
        options,
      }
    );
    const select = `SUM(\`${this.tableName}\`.\`${field}\`) AS sum_value`;
    const { sql, params } = this.buildSelectQuery(dataset, options, select);
    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    const sumValue = rows[0]?.sum_value || 0;
    this.orm.logger.info(
      `[Model:sum] Sum for ${field}: ${sumValue} in ${this.name} in dataset ${dataset}`
    );
    return sumValue;
  }

  static async average(
    dataset: string,
    field: string,
    options: FindOptions = {}
  ): Promise<number | null> {
    this.orm.logger.info(
      `[Model:average] Getting average for field ${field} in ${this.name} in dataset ${dataset}`,
      {
        field,
        options,
      }
    );
    const select = `AVG(\`${this.tableName}\`.\`${field}\`) AS avg_value`;
    const { sql, params } = this.buildSelectQuery(dataset, options, select);
    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    const avgValue = rows[0]?.avg_value || null;
    this.orm.logger.info(
      `[Model:average] Average for ${field}: ${avgValue} in ${this.name} in dataset ${dataset}`
    );
    return avgValue;
  }

  private static resolveDefault(value: any): any {
    if (value === DataTypes.NOW || value === "CURRENT_TIMESTAMP()") {
      return new Date();
    } else if (value === DataTypes.UUIDV4 || value === "GENERATE_UUID()") {
      return crypto.randomUUID();
    } else if (
      value === DataTypes.NOW_DATETIME ||
      value === "CURRENT_DATETIME()"
    ) {
      const now = new Date();
      return now.toISOString().slice(0, 19).replace("T", " ");
    }
    return value;
  }

  // Add this helper method to the Model class
  private static async checkForDuplicatePrimaryKeys(
    dataset: string,
    primaryKeyValues: any[]
  ): Promise<Set<any>> {
    if (!primaryKeyValues.length) return new Set();

    this.orm.logger.info(
      `[Model:checkForDuplicatePrimaryKeys] Checking for duplicate primary keys for ${this.name}`,
      { primaryKeyValues }
    );

    const paramNames = primaryKeyValues.map((_, index) => `@pk${index}`);
    const params = primaryKeyValues.reduce((acc, value, index) => {
      acc[`pk${index}`] = value;
      return acc;
    }, {} as Record<string, any>);

    const sql = `SELECT \`${this.primaryKey}\` FROM \`${dataset}.${
      this.tableName
    }\` 
               WHERE \`${this.primaryKey}\` IN (${paramNames.join(", ")})`;

    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    return new Set(rows.map((row: any) => row[this.primaryKey]));
  }

  // Update the create method
  static async create(
    dataset: string,
    data: Record<string, any>
  ): Promise<any> {
    this.orm.logger.info(
      `[Model:create] Creating record for ${this.name} in dataset ${dataset}`,
      { data }
    );

    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[Model:create] Free tier mode: CREATE (INSERT) not allowed."
      );
      throw new Error("Free tier mode: CREATE (INSERT) not allowed.");
    }

    const filledData: Record<string, any> = {};
    for (const [field, attr] of Object.entries(this.attributes)) {
      if (field in data) {
        filledData[field] = data[field];
      } else if (attr.defaultValue !== undefined) {
        filledData[field] = this.resolveDefault(attr.defaultValue);
      } else if (attr.allowNull === false) {
        this.orm.logger.error(
          `[Model:create] Missing required field ${field} for ${this.name} in dataset ${dataset}`
        );
        throw new Error(`Missing required field ${field}`);
      }
    }

    // Check for duplicate primary key
    const primaryKeyValue = filledData[this.primaryKey];
    if (primaryKeyValue !== undefined && primaryKeyValue !== null) {
      const existingKeys = await this.checkForDuplicatePrimaryKeys(dataset, [
        primaryKeyValue,
      ]);
      if (existingKeys.has(primaryKeyValue)) {
        this.orm.logger.error(
          `[Model:create] Duplicate primary key ${this.primaryKey}=${primaryKeyValue} for ${this.name} in dataset ${dataset}`
        );
        throw new Error(`Duplicate primary key value: ${primaryKeyValue}`);
      }
    }

    const table = this.orm.bigquery.dataset(dataset).table(this.tableName);
    await table.insert([filledData]);
    this.orm.logger.info(
      `[Model:create] Record created for ${this.name} in dataset ${dataset}`
    );
    return filledData;
  }

  // Update the bulkCreate method
  static async bulkCreate(
    dataset: string,
    data: Record<string, any>[],
    options: BulkCreateOptions = {}
  ): Promise<any[]> {
    this.orm.logger.info(
      `[Model:bulkCreate] Creating ${data.length} records for ${this.name} in dataset ${dataset}`
    );

    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[Model:bulkCreate] Free tier mode: BULK CREATE (INSERT) not allowed."
      );
      throw new Error("Free tier mode: BULK CREATE (INSERT) not allowed.");
    }

    if (!data.length) {
      this.orm.logger.info("[Model:bulkCreate] No records to create, skipping");
      return [];
    }

    const filledData = data.map((record) => {
      const filled: Record<string, any> = {};
      for (const [field, attr] of Object.entries(this.attributes)) {
        if (field in record) {
          filled[field] = record[field];
        } else if (attr.defaultValue !== undefined) {
          filled[field] = this.resolveDefault(attr.defaultValue);
        } else if (attr.allowNull === false && options.validate !== false) {
          this.orm.logger.error(
            `[Model:bulkCreate] Missing required field ${field} in bulk create record for ${this.name} in dataset ${dataset}`
          );
          throw new Error(
            `Missing required field ${field} in bulk create record`
          );
        }
      }
      return filled;
    });

    // Check for duplicate primary keys in batch
    const primaryKeyValues = filledData
      .map((record) => record[this.primaryKey])
      .filter((value) => value !== undefined && value !== null);

    if (primaryKeyValues.length > 0) {
      const existingKeys = await this.checkForDuplicatePrimaryKeys(
        dataset,
        primaryKeyValues
      );

      // Check for duplicates within the current batch
      const batchKeySet = new Set();
      const duplicatesInBatch: any[] = [];

      for (const record of filledData) {
        const keyValue = record[this.primaryKey];
        if (keyValue !== undefined && keyValue !== null) {
          if (batchKeySet.has(keyValue)) {
            duplicatesInBatch.push(keyValue);
          } else {
            batchKeySet.add(keyValue);
          }
        }
      }

      if (duplicatesInBatch.length > 0) {
        this.orm.logger.error(
          `[Model:bulkCreate] Duplicate primary keys within batch: ${duplicatesInBatch.join(
            ", "
          )} for ${this.name}`
        );
        throw new Error(
          `Duplicate primary keys within batch: ${duplicatesInBatch.join(", ")}`
        );
      }

      // Check against existing records
      const conflictingKeys = Array.from(existingKeys);
      if (conflictingKeys.length > 0) {
        this.orm.logger.error(
          `[Model:bulkCreate] Duplicate primary keys with existing records: ${conflictingKeys.join(
            ", "
          )} for ${this.name}`
        );
        throw new Error(
          `Duplicate primary keys with existing records: ${conflictingKeys.join(
            ", "
          )}`
        );
      }
    }

    const table = this.orm.bigquery.dataset(dataset).table(this.tableName);

    // Use batch insertion with error handling
    try {
      await table.insert(filledData);
      this.orm.logger.info(
        `[Model:bulkCreate] ${filledData.length} records created for ${this.name} in dataset ${dataset}`
      );
      return options.returning ? filledData : [];
    } catch (error: any) {
      // Handle potential race condition where duplicates might still occur
      if (
        (error.message && error.message.includes("duplicate")) ||
        error.message.includes("already exists")
      ) {
        this.orm.logger.error(
          `[Model:bulkCreate] Race condition detected: duplicates found during insertion for ${this.name}`
        );
        throw new Error(
          "Duplicate primary keys detected during insertion (race condition)"
        );
      }
      throw error;
    }
  }
  static async update(
    dataset: string,
    data: Record<string, any>,
    options: UpdateOptions
  ): Promise<number> {
    this.orm.logger.info(
      `[Model:update] Updating records for ${this.name} in dataset ${dataset}`,
      {
        data,
        options,
      }
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[Model:update] Free tier mode: UPDATE not allowed."
      );
      throw new Error("Free tier mode: UPDATE not allowed.");
    }
    const setClauses = Object.entries(data)
      .map(([field]) => `\`${field}\` = @set_${field}`)
      .join(", ");
    const setValues = Object.entries(data).reduce(
      (acc, [field, value]) => ({ ...acc, [`set_${field}`]: value }),
      {}
    );
    const { clause: whereClause, params: whereValues } = buildWhereClause(
      options.where
    );
    const sql = `UPDATE \`${dataset}.${
      this.tableName
    }\` SET ${setClauses} WHERE ${whereClause || "TRUE"}`;
    const allParams = { ...setValues, ...whereValues };
    const [job] = await this.orm.bigquery.createQueryJob({
      query: sql,
      params: allParams,
    });
    await job.getQueryResults();
    const [metadata] = await job.getMetadata();
    const affectedRows = Number(
      metadata.statistics?.query?.numDmlAffectedRows || 0
    );
    this.orm.logger.info(
      `[Model:update] Updated ${affectedRows} records for ${this.name} in dataset ${dataset}`
    );
    return affectedRows;
  }

  static async destroy(
    dataset: string,
    options: DestroyOptions
  ): Promise<number> {
    this.orm.logger.info(
      `[Model:destroy] Deleting records for ${this.name} in dataset ${dataset}`,
      {
        options,
      }
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[Model:destroy] Free tier mode: DESTROY (DELETE) not allowed."
      );
      throw new Error("Free tier mode: DESTROY (DELETE) not allowed.");
    }
    const { clause, params } = buildWhereClause(options.where);
    const sql = `DELETE FROM \`${dataset}.${this.tableName}\` WHERE ${
      clause || "TRUE"
    }`;

    this.orm.logger.info(
      `[Model:destroy] Executing query: ${sql} in dataset ${dataset}`,
      { params }
    );
    try {
      const [job] = await this.orm.bigquery.createQueryJob({
        query: sql,
        params,
      });
      await job.getQueryResults();
      const [metadata] = await job.getMetadata();
      const affectedRows = Number(
        metadata.statistics?.query?.numDmlAffectedRows || 0
      );
      this.orm.logger.info(
        `[Model:destroy] Deleted ${affectedRows} records for ${this.name} in dataset ${dataset}`
      );
      return affectedRows;
    } catch (error: any) {
      this.orm.logger.error(
        `[Model:destroy] Failed to delete records for ${this.name} in dataset ${dataset}`,
        {
          error: error.message,
          stack: error.stack,
          sql,
          params,
        }
      );
      throw error;
    }
  }

  static async increment(
    dataset: string,
    fields: string | string[],
    options: { by?: number; where: WhereOptions }
  ): Promise<number> {
    this.orm.logger.info(
      `[Model:increment] Incrementing fields for ${this.name} in dataset ${dataset}`,
      { fields, options }
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[Model:increment] Free tier mode: INCREMENT (UPDATE) not allowed."
      );
      throw new Error("Free tier mode: INCREMENT (UPDATE) not allowed.");
    }
    const by = options.by || 1;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    const setClauses = fieldArray
      .map((field) => `\`${field}\` = \`${field}\` + ${by}`)
      .join(", ");
    const { clause: whereClause, params: whereValues } = buildWhereClause(
      options.where
    );
    const sql = `UPDATE \`${dataset}.${
      this.tableName
    }\` SET ${setClauses} WHERE ${whereClause || "TRUE"}`;
    const [job] = await this.orm.bigquery.createQueryJob({
      query: sql,
      params: whereValues,
    });
    await job.getQueryResults();
    const [metadata] = await job.getMetadata();
    const affectedRows = Number(
      metadata.statistics?.query?.numDmlAffectedRows || 0
    );
    this.orm.logger.info(
      `[Model:increment] Incremented ${affectedRows} records for ${this.name} in dataset ${dataset}`
    );
    return affectedRows;
  }

  static async decrement(
    dataset: string,
    fields: string | string[],
    options: { by?: number; where: WhereOptions }
  ): Promise<number> {
    this.orm.logger.info(
      `[Model:decrement] Decrementing fields for ${this.name} in dataset ${dataset}`,
      { fields, options }
    );
    return this.increment(dataset, fields, {
      ...options,
      by: -(options.by || 1),
    });
  }

  static async truncate(dataset: string): Promise<void> {
    this.orm.logger.info(
      `[Model:truncate] Truncating table for ${this.name} in dataset ${dataset}`
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[Model:truncate] Free tier mode: TRUNCATE not allowed."
      );
      throw new Error("Free tier mode: TRUNCATE not allowed.");
    }
    const sql = `TRUNCATE TABLE \`${dataset}.${this.tableName}\``;
    await this.orm.bigquery.query(sql);
    this.orm.logger.info(
      `[Model:truncate] Table truncated for ${this.name} in dataset ${dataset}`
    );
  }

  static async describe(): Promise<Record<string, DataType>> {
    this.orm.logger.info(`[Model:describe] Describing table for ${this.name}`);
    return { ...this.attributes };
  }

  private static buildSelectQuery(
    dataset: string,
    options: FindOptions,
    selectOverride?: string
  ): { sql: string; params: Record<string, any> } {
    this.orm.logger.info(
      `[Model:buildSelectQuery] Building query for ${this.name} in dataset ${dataset}`,
      { options, selectOverride }
    );
    const mainAlias = this.tableName;
    let sql = `FROM \`${dataset}.${this.tableName}\` AS \`${mainAlias}\``;
    const params: Record<string, any> = {};
    const whereClauses: string[] = [];

    if (options.include) {
      for (const inc of options.include) {
        const as = inc.as || inc.model.tableName;
        const assoc = Object.values(this.associations).find(
          (a) => a.as === as && a.target === inc.model
        );
        if (!assoc) {
          this.orm.logger.error(
            `[Model:buildSelectQuery] Association not found for ${inc.model.name} in ${this.name}`,
            { associations: Object.keys(this.associations) }
          );
          throw new Error(`Association not found for ${inc.model.name}`);
        }
        const joinType = inc.required ? "INNER JOIN" : "LEFT OUTER JOIN";
        let joinOn: string;
        if (assoc.type === "belongsTo") {
          joinOn = `\`${mainAlias}\`.\`${assoc.foreignKey}\` = \`${as}\`.\`${inc.model.primaryKey}\``;
          sql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON ${joinOn}`;
        } else if (assoc.type === "hasOne" || assoc.type === "hasMany") {
          joinOn = `\`${mainAlias}\`.\`${this.primaryKey}\` = \`${as}\`.\`${assoc.foreignKey}\``;
          sql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON ${joinOn}`;
        } else if (assoc.type === "belongsToMany") {
          if (!assoc.through || !assoc.otherKey) {
            this.orm.logger.error(
              `[Model:buildSelectQuery] Through model and otherKey required for belongsToMany in ${this.name}`,
              { assoc }
            );
            throw new Error(
              "Through model and otherKey required for belongsToMany"
            );
          }
          const throughAs = `${as}_through`;
          const throughTable = assoc.through.tableName;
          // Use assoc.foreignKey for both sides of the first join
          joinOn = `\`${mainAlias}\`.\`${assoc.foreignKey}\` = \`${throughAs}\`.\`${assoc.foreignKey}\``;
          sql += ` ${joinType} \`${dataset}.${throughTable}\` AS \`${throughAs}\` ON ${joinOn}`;
          joinOn = `\`${throughAs}\`.\`${assoc.otherKey}\` = \`${as}\`.\`${inc.model.primaryKey}\``;
          sql += ` ${joinType} \`${dataset}.${inc.model.tableName}\` AS \`${as}\` ON ${joinOn}`;
        }

        if (inc.where) {
          const { clause, params: incParams } = buildWhereClause(inc.where);
          const prefixedClause = clause.replace(/`([^`]+)`/g, `\`${as}\`.$1`);
          whereClauses.push(prefixedClause);
          Object.assign(params, incParams);
        }
      }
    }

    let mainWhere = "";
    if (options.where) {
      const { clause, params: mParams } = buildWhereClause(options.where);
      mainWhere = clause;
      Object.assign(params, mParams);
    }

    let whereClause = [mainWhere, ...whereClauses]
      .filter((c) => c)
      .join(" AND ");
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }

    let selectClause: string[] = [];
    if (selectOverride) {
      selectClause.push(selectOverride);
    } else {
      const mainAttributes = options.attributes || Object.keys(this.attributes);
      for (const field of mainAttributes) {
        selectClause.push(
          `\`${mainAlias}\`.\`${field}\` AS \`${mainAlias}_${field}\``
        );
      }
      if (options.include) {
        for (const inc of options.include) {
          const as = inc.as || inc.model.tableName;
          const incAttributes = inc.attributes?.length
            ? inc.attributes
            : Object.keys(inc.model.attributes);
          for (const field of incAttributes) {
            selectClause.push(`\`${as}\`.\`${field}\` AS \`${as}_${field}\``);
          }
        }
      }
    }

    if (options.distinct) {
      sql = `SELECT DISTINCT ${selectClause.join(", ")} ${sql}`;
    } else {
      sql = `SELECT ${selectClause.join(", ")} ${sql}`;
    }

    if (options.group) {
      sql += ` GROUP BY ${options.group.map((g) => `\`${g}\``).join(", ")}`;
    }

    if (options.order) {
      sql += ` ORDER BY ${options.order
        .map(([field, dir]) => `\`${mainAlias}\`.\`${field}\` ${dir}`)
        .join(", ")}`;
    }

    if (options.limit) {
      sql += ` LIMIT ${options.limit}`;
    }
    if (options.offset) {
      sql += ` OFFSET ${options.offset}`;
    }

    this.orm.logger.info(
      `[Model:buildSelectQuery] Generated SQL for ${this.name} in dataset ${dataset}`,
      { sql, params }
    );
    return { sql, params };
  }

  private static nestAssociations(
    rows: any[],
    includes: IncludeOptions[]
  ): any[] {
    this.orm.logger.info(
      `[Model:nestAssociations] Nesting associations for ${this.name}`,
      { includes }
    );

    if (!includes.length) {
      return rows.map((row) => {
        const result: any = {};
        for (const [key, value] of Object.entries(row)) {
          if (key.startsWith(`${this.tableName}_`)) {
            result[key.replace(`${this.tableName}_`, "")] = value;
          }
        }
        return result;
      });
    }

    const parentMap = new Map<any, any>();

    for (const row of rows) {
      const parentPKValue = row[`${this.tableName}_${this.primaryKey}`];
      if (parentPKValue == null) continue;

      let parent = parentMap.get(parentPKValue);
      if (!parent) {
        parent = {};
        for (const field in this.attributes) {
          parent[field] = row[`${this.tableName}_${field}`];
        }
        for (const inc of includes) {
          const as = inc.as || inc.model.tableName;
          const assoc = Object.values(this.associations).find(
            (a) => a.as === as
          );
          if (assoc) {
            parent[as] =
              assoc.type === "hasMany" || assoc.type === "belongsToMany"
                ? []
                : null;
          }
        }
        parentMap.set(parentPKValue, parent);
      }

      for (const inc of includes) {
        const as = inc.as || inc.model.tableName;
        const assoc = Object.values(this.associations).find((a) => a.as === as);
        if (!assoc) continue;

        const childPK = row[`${as}_${inc.model.primaryKey}`];
        if (childPK == null) continue;

        const child: any = {};
        // Use only requested attributes + PK internally
        const selectedFields = inc.attributes
          ? Array.from(new Set([inc.model.primaryKey, ...inc.attributes]))
          : Object.keys(inc.model.attributes);

        for (const field of selectedFields) {
          if (
            field === inc.model.primaryKey &&
            inc.attributes &&
            !inc.attributes.includes(field)
          ) {
            // skip PK if user didn’t request it
            continue;
          }
          child[field] = row[`${as}_${field}`];
        }

        if (assoc.type === "hasMany" || assoc.type === "belongsToMany") {
          if (
            !parent[as].some((c: any) => c[inc.model.primaryKey] === childPK)
          ) {
            parent[as].push(child);
          }
        } else {
          parent[as] = child;
        }
      }
    }

    const result = Array.from(parentMap.values());
    this.orm.logger.info(
      `[Model:nestAssociations] Nested ${result.length} records for ${this.name}`
    );
    return result;
  }
}
