// model.ts
import { BigQuery, Job } from "@google-cloud/bigquery";
import { BigQueryORM } from "./bigQueryORM";
import { Op, Operator } from "./op";
import { DataType, DataTypes } from "./dataTypes";
import { buildWhereClause } from "./utils";

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
}

export interface Association {
  type: "hasOne" | "hasMany" | "belongsTo" | "belongsToMany";
  target: typeof Model;
  foreignKey: string;
  otherKey?: string;
  as?: string;
  through?: typeof Model;
}

export abstract class Model {
  static orm: BigQueryORM;
  static tableName: string;
  static primaryKey: string = "id";
  static attributes: Record<string, DataType>;
  static associations: Record<string, Association> = {};

  static init(
    attributes: Record<string, DataType>,
    options: { orm: BigQueryORM; tableName?: string; primaryKey?: string }
  ) {
    console.log(`[Model.init] Starting initialization for model: ${this.name}`);
    console.log(`[Model.init] Received attributes:`, Object.keys(attributes));
    console.log(`[Model.init] Options:`, options);

    this.orm = options.orm;
    console.log(`[Model.init] Set ORM instance`);

    this.attributes = attributes;
    console.log(`[Model.init] Set attributes`);

    this.tableName = options.tableName || this.name.toLowerCase();
    console.log(`[Model.init] Set tableName: ${this.tableName}`);

    this.primaryKey =
      options.primaryKey ||
      Object.keys(attributes).find((key) => attributes[key].primaryKey) ||
      "id";
    console.log(`[Model.init] Set primaryKey: ${this.primaryKey}`);
    console.log(`[Model.init] Initialization complete for model: ${this.name}`);
  }

  static belongsTo(
    target: typeof Model,
    options: { foreignKey?: string; as?: string } = {}
  ) {
    console.log(
      `[Model.belongsTo] Setting up belongsTo relationship from ${this.name} to ${target.name}`
    );
    console.log(`[Model.belongsTo] Options:`, options);

    const foreignKey = options.foreignKey || `${target.name.toLowerCase()}Id`;
    console.log(`[Model.belongsTo] Computed foreignKey: ${foreignKey}`);

    const as = options.as || target.name.toLowerCase();
    console.log(`[Model.belongsTo] Computed alias: ${as}`);

    this.associations[as] = { type: "belongsTo", target, foreignKey, as };
    console.log(`[Model.belongsTo] Added association to associations map`);

    if (!this.attributes[foreignKey]) {
      console.log(
        `[Model.belongsTo] Foreign key ${foreignKey} not found in attributes, adding as INTEGER`
      );
      this.attributes[foreignKey] = DataTypes.INTEGER();
    } else {
      console.log(
        `[Model.belongsTo] Foreign key ${foreignKey} already exists in attributes`
      );
    }
    console.log(`[Model.belongsTo] BelongsTo relationship setup complete`);
  }

  static hasOne(
    target: typeof Model,
    options: { foreignKey?: string; as?: string } = {}
  ) {
    console.log(
      `[Model.hasOne] Setting up hasOne relationship from ${this.name} to ${target.name}`
    );
    console.log(`[Model.hasOne] Options:`, options);

    const foreignKey = options.foreignKey || `${this.name.toLowerCase()}Id`;
    console.log(`[Model.hasOne] Computed foreignKey: ${foreignKey}`);

    const as = options.as || target.name.toLowerCase();
    console.log(`[Model.hasOne] Computed alias: ${as}`);

    this.associations[as] = { type: "hasOne", target, foreignKey, as };
    console.log(`[Model.hasOne] Added association to associations map`);
    console.log(`[Model.hasOne] HasOne relationship setup complete`);
  }

  static hasMany(
    target: typeof Model,
    options: { foreignKey?: string; as?: string } = {}
  ) {
    console.log(
      `[Model.hasMany] Setting up hasMany relationship from ${this.name} to ${target.name}`
    );
    console.log(`[Model.hasMany] Options:`, options);

    const foreignKey = options.foreignKey || `${this.name.toLowerCase()}Id`;
    console.log(`[Model.hasMany] Computed foreignKey: ${foreignKey}`);

    const as = options.as || `${target.name.toLowerCase()}s`;
    console.log(`[Model.hasMany] Computed alias: ${as}`);

    this.associations[as] = { type: "hasMany", target, foreignKey, as };
    console.log(`[Model.hasMany] Added association to associations map`);
    console.log(`[Model.hasMany] HasMany relationship setup complete`);
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
    console.log(
      `[Model.belongsToMany] Setting up belongsToMany relationship from ${this.name} to ${target.name}`
    );
    console.log(`[Model.belongsToMany] Through model: ${options.through.name}`);
    console.log(`[Model.belongsToMany] Options:`, options);

    const foreignKey = options.foreignKey || `${this.name.toLowerCase()}Id`;
    console.log(`[Model.belongsToMany] Computed foreignKey: ${foreignKey}`);

    const otherKey = options.otherKey || `${target.name.toLowerCase()}Id`;
    console.log(`[Model.belongsToMany] Computed otherKey: ${otherKey}`);

    const as = options.as || `${target.name.toLowerCase()}s`;
    console.log(`[Model.belongsToMany] Computed alias: ${as}`);

    this.associations[as] = {
      type: "belongsToMany",
      target,
      foreignKey,
      otherKey,
      through: options.through,
      as,
    };
    console.log(`[Model.belongsToMany] Added association to associations map`);
    console.log(
      `[Model.belongsToMany] BelongsToMany relationship setup complete`
    );
  }

  static async findAll(options: FindOptions = {}): Promise<any[]> {
    console.log(`[Model.findAll] Starting findAll for ${this.name}`);
    console.log(`[Model.findAll] Options:`, options);

    const { sql, params } = this.buildSelectQuery(options);
    console.log(`[Model.findAll] Built query SQL:`, sql);
    console.log(`[Model.findAll] Query parameters:`, params);

    try {
      console.log(`[Model.findAll] Executing BigQuery query`);
      const [rows] = await this.orm.bigquery.query({ query: sql, params });
      console.log(
        `[Model.findAll] Query executed successfully, got ${rows.length} rows`
      );

      if (options.raw) {
        console.log(`[Model.findAll] Raw mode enabled, returning raw rows`);
        return rows;
      }

      console.log(
        `[Model.findAll] Processing associations and nesting results`
      );
      const nestedResults = this.nestAssociations(rows, options.include || []);
      console.log(
        `[Model.findAll] Nested results processed, returning ${nestedResults.length} records`
      );
      return nestedResults;
    } catch (err: any) {
      console.error(
        `[Model.findAll] FindAll query failed for ${this.name}:`,
        err.message
      );
      console.error(`[Model.findAll] Failed SQL:`, sql);
      console.error(`[Model.findAll] Failed params:`, params);
      throw err;
    }
  }

  static async findOne(options: FindOptions = {}): Promise<any | null> {
    console.log(`[Model.findOne] Starting findOne for ${this.name}`);
    console.log(`[Model.findOne] Options:`, options);

    try {
      console.log(`[Model.findOne] Calling findAll with limit 1`);
      const results = await this.findAll({ ...options, limit: 1 });
      console.log(`[Model.findOne] FindAll returned ${results.length} results`);

      const result = results[0] || null;
      console.log(
        `[Model.findOne] Returning result:`,
        result ? "found" : "null"
      );
      return result;
    } catch (err: any) {
      console.error(
        `[Model.findOne] FindOne query failed for ${this.name}:`,
        err.message
      );
      throw err;
    }
  }

  static async findByPk(
    pk: any,
    options: FindOptions = {}
  ): Promise<any | null> {
    console.log(`[Model.findByPk] Starting findByPk for ${this.name}`);
    console.log(`[Model.findByPk] Primary key value:`, pk);
    console.log(`[Model.findByPk] Primary key field:`, this.primaryKey);
    console.log(`[Model.findByPk] Options:`, options);

    const whereClause = { [this.primaryKey]: pk };
    console.log(`[Model.findByPk] Built where clause:`, whereClause);

    return this.findOne({ ...options, where: whereClause });
  }

  static async count(options: FindOptions = {}): Promise<number> {
    console.log(`[Model.count] Starting count for ${this.name}`);
    console.log(`[Model.count] Options:`, options);

    const select = `COUNT(DISTINCT \`${this.tableName}\`.\`${this.primaryKey}\`) AS count`;
    console.log(`[Model.count] Count select clause:`, select);

    const { sql, params } = this.buildSelectQuery(options, select);
    console.log(`[Model.count] Built count query SQL:`, sql);
    console.log(`[Model.count] Query parameters:`, params);

    try {
      console.log(`[Model.count] Executing count query`);
      const [rows] = await this.orm.bigquery.query({ query: sql, params });
      console.log(`[Model.count] Count query executed, raw result:`, rows);

      const count = rows[0]?.count || 0;
      console.log(`[Model.count] Extracted count:`, count);
      return count;
    } catch (err: any) {
      console.error(
        `[Model.count] Count query failed for ${this.name}:`,
        err.message
      );
      console.error(`[Model.count] Failed SQL:`, sql);
      console.error(`[Model.count] Failed params:`, params);
      throw err;
    }
  }

  private static resolveDefault(value: any): any {
    console.log(`[Model.resolveDefault] Resolving default value:`, value);

    if (value === DataTypes.NOW || value === "CURRENT_TIMESTAMP()") {
      const now = new Date();
      console.log(`[Model.resolveDefault] Resolved to current timestamp:`, now);
      return now;
    } else if (value === DataTypes.UUIDV4 || value === "GENERATE_UUID()") {
      const uuid = crypto.randomUUID();
      console.log(`[Model.resolveDefault] Generated UUID:`, uuid);
      return uuid;
    } else {
      console.log(`[Model.resolveDefault] Using value as-is:`, value);
      return value;
    }
  }

  static async create(data: Record<string, any>): Promise<any> {
    console.log(`[Model.create] Starting create for ${this.name}`);
    console.log(`[Model.create] Input data:`, data);

    if (this.orm.config.freeTierMode) {
      console.log(`[Model.create] Free tier mode detected, throwing error`);
      throw new Error(
        "Free tier mode: CREATE (INSERT) not allowed. Enable billing at https://console.cloud.google.com/billing."
      );
    }

    console.log(`[Model.create] Processing attributes and defaults`);
    const filledData: Record<string, any> = {};

    for (const [field, attr] of Object.entries(this.attributes)) {
      console.log(`[Model.create] Processing field: ${field}`);

      if (field in data) {
        console.log(`[Model.create] Field ${field} provided in data`);
        filledData[field] = data[field];
      } else if (attr.defaultValue !== undefined) {
        console.log(
          `[Model.create] Field ${field} using default value:`,
          attr.defaultValue
        );
        filledData[field] = this.resolveDefault(attr.defaultValue);
      } else if (attr.allowNull === false) {
        console.log(`[Model.create] Field ${field} is required but missing`);
        throw new Error(`Missing required field ${field}`);
      } else {
        console.log(`[Model.create] Field ${field} omitted (nullable)`);
      }
    }

    console.log(`[Model.create] Final data to insert:`, filledData);

    const table = this.orm.bigquery
      .dataset(this.orm.config.dataset)
      .table(this.tableName);
    console.log(
      `[Model.create] Got table reference: ${this.orm.config.dataset}.${this.tableName}`
    );

    try {
      console.log(`[Model.create] Inserting record into BigQuery`);
      await table.insert([filledData]);
      console.log(`[Model.create] Insert successful`);

      if (this.orm.config.logging)
        console.log(`[Model.create] Created record in ${this.tableName}`);
      return filledData;
    } catch (err: any) {
      console.error(
        `[Model.create] Failed to create record in ${this.tableName}:`,
        err.message
      );
      console.error(`[Model.create] Failed data:`, filledData);
      throw err;
    }
  }

  static async bulkCreate(data: Record<string, any>[]): Promise<void> {
    console.log(`[Model.bulkCreate] Starting bulkCreate for ${this.name}`);
    console.log(`[Model.bulkCreate] Number of records:`, data.length);

    if (this.orm.config.freeTierMode) {
      console.log(`[Model.bulkCreate] Free tier mode detected, throwing error`);
      throw new Error(
        "Free tier mode: BULK CREATE (INSERT) not allowed. Enable billing at https://console.cloud.google.com/billing."
      );
    }

    if (data.length === 0) {
      console.log(`[Model.bulkCreate] No data provided, returning early`);
      return;
    }

    console.log(`[Model.bulkCreate] Processing ${data.length} records`);
    const filledData = data.map((record, index) => {
      console.log(`[Model.bulkCreate] Processing record ${index + 1}`);
      const filled: Record<string, any> = {};

      for (const [field, attr] of Object.entries(this.attributes)) {
        if (field in record) {
          filled[field] = record[field];
        } else if (attr.defaultValue !== undefined) {
          filled[field] = this.resolveDefault(attr.defaultValue);
        } else if (attr.allowNull === false) {
          console.log(
            `[Model.bulkCreate] Record ${
              index + 1
            } missing required field: ${field}`
          );
          throw new Error(
            `Missing required field ${field} in bulk create record`
          );
        }
      }
      return filled;
    });

    console.log(
      `[Model.bulkCreate] Processed all records, ready for bulk insert`
    );

    const table = this.orm.bigquery
      .dataset(this.orm.config.dataset)
      .table(this.tableName);
    console.log(
      `[Model.bulkCreate] Got table reference: ${this.orm.config.dataset}.${this.tableName}`
    );

    try {
      console.log(`[Model.bulkCreate] Executing bulk insert`);
      await table.insert(filledData);
      console.log(`[Model.bulkCreate] Bulk insert successful`);

      if (this.orm.config.logging)
        console.log(
          `[Model.bulkCreate] Bulk created ${data.length} records in ${this.tableName}`
        );
    } catch (err: any) {
      console.error(
        `[Model.bulkCreate] Failed to bulk create records in ${this.tableName}:`,
        err.message
      );
      throw err;
    }
  }

  static async update(
    data: Record<string, any>,
    options: { where: WhereOptions }
  ): Promise<number> {
    console.log(`[Model.update] Starting update for ${this.name}`);
    console.log(`[Model.update] Update data:`, data);
    console.log(`[Model.update] Where options:`, options.where);

    if (this.orm.config.freeTierMode) {
      console.log(`[Model.update] Free tier mode detected, throwing error`);
      throw new Error(
        "Free tier mode: UPDATE not allowed. Enable billing at https://console.cloud.google.com/billing."
      );
    }

    console.log(`[Model.update] Building SET clauses`);
    const setClauses = Object.entries(data)
      .map(([field]) => `\`${field}\` = @set_${field}`)
      .join(", ");
    console.log(`[Model.update] SET clauses:`, setClauses);

    const setValues = Object.entries(data).reduce(
      (acc, [field, value]) => ({ ...acc, [`set_${field}`]: value }),
      {}
    );
    console.log(`[Model.update] SET values:`, setValues);

    console.log(`[Model.update] Building WHERE clause`);
    const { clause: whereClause, params: whereValues } = buildWhereClause(
      options.where
    );
    console.log(`[Model.update] WHERE clause:`, whereClause);
    console.log(`[Model.update] WHERE values:`, whereValues);

    const sql = `UPDATE \`${this.orm.config.dataset}.${
      this.tableName
    }\` SET ${setClauses} WHERE ${whereClause || "TRUE"}`;
    console.log(`[Model.update] Final SQL:`, sql);

    const allParams = { ...setValues, ...whereValues };
    console.log(`[Model.update] All parameters:`, allParams);

    try {
      console.log(`[Model.update] Creating query job`);
      const [job] = await this.orm.bigquery.createQueryJob({
        query: sql,
        params: allParams,
      });
      console.log(`[Model.update] Query job created, waiting for results`);

      await job.getQueryResults();
      console.log(`[Model.update] Query job completed`);

      const [metadata] = await job.getMetadata();
      console.log(`[Model.update] Job metadata retrieved`);

      const affectedRows = Number(
        metadata.statistics?.query?.numDmlAffectedRows || 0
      );
      console.log(`[Model.update] Affected rows:`, affectedRows);

      if (this.orm.config.logging)
        console.log(
          `[Model.update] Updated ${affectedRows} rows in ${this.tableName}`
        );
      return affectedRows;
    } catch (err: any) {
      if (
        err.message.includes("UPDATE or DELETE statement over table") &&
        err.message.includes("streaming buffer")
      ) {
        console.log(`[Model.update] Streaming buffer conflict detected`);
        throw new Error(
          `Cannot UPDATE rows currently in the streaming buffer for table ${this.tableName}. Please wait a few minutes before retrying.`
        );
      }
      console.error(
        `[Model.update] Failed to update records in ${this.tableName}:`,
        err.message
      );
      console.error(`[Model.update] Failed SQL:`, sql);
      console.error(`[Model.update] Failed params:`, allParams);
      throw err;
    }
  }

  static async destroy(options: { where: WhereOptions }): Promise<number> {
    console.log(`[Model.destroy] Starting destroy for ${this.name}`);
    console.log(`[Model.destroy] Where options:`, options.where);

    if (this.orm.config.freeTierMode) {
      console.log(`[Model.destroy] Free tier mode detected, throwing error`);
      throw new Error(
        "Free tier mode: DESTROY (DELETE) not allowed. Enable billing at https://console.cloud.google.com/billing."
      );
    }

    console.log(`[Model.destroy] Building WHERE clause`);
    const { clause, params } = buildWhereClause(options.where);
    console.log(`[Model.destroy] WHERE clause:`, clause);
    console.log(`[Model.destroy] WHERE params:`, params);

    const sql = `DELETE FROM \`${this.orm.config.dataset}.${
      this.tableName
    }\` WHERE ${clause || "TRUE"}`;
    console.log(`[Model.destroy] Final SQL:`, sql);

    try {
      console.log(`[Model.destroy] Creating query job`);
      const [job] = await this.orm.bigquery.createQueryJob({
        query: sql,
        params,
      });
      console.log(`[Model.destroy] Query job created, waiting for results`);

      await job.getQueryResults();
      console.log(`[Model.destroy] Query job completed`);

      const [metadata] = await job.getMetadata();
      console.log(`[Model.destroy] Job metadata retrieved`);

      const affectedRows = Number(
        metadata.statistics?.query?.numDmlAffectedRows || 0
      );
      console.log(`[Model.destroy] Affected rows:`, affectedRows);

      if (this.orm.config.logging)
        console.log(
          `[Model.destroy] Deleted ${affectedRows} rows from ${this.tableName}`
        );

      return affectedRows;
    } catch (err: any) {
      if (
        err.message.includes("UPDATE or DELETE statement over table") &&
        err.message.includes("streaming buffer")
      ) {
        console.log(`[Model.destroy] Streaming buffer conflict detected`);
        throw new Error(
          `Cannot DELETE rows currently in the streaming buffer for table ${this.tableName}. Please wait a few minutes before retrying.`
        );
      }
      console.error(
        `[Model.destroy] Failed to delete records from ${this.tableName}:`,
        err.message
      );
      console.error(`[Model.destroy] Failed SQL:`, sql);
      console.error(`[Model.destroy] Failed params:`, params);
      throw err;
    }
  }

  static async increment(
    fields: string | string[],
    options: { by?: number; where: WhereOptions }
  ): Promise<number> {
    console.log(`[Model.increment] Starting increment for ${this.name}`);
    console.log(`[Model.increment] Fields:`, fields);
    console.log(`[Model.increment] Options:`, options);

    if (this.orm.config.freeTierMode) {
      console.log(`[Model.increment] Free tier mode detected, throwing error`);
      throw new Error(
        "Free tier mode: INCREMENT (UPDATE) not allowed. Enable billing at https://console.cloud.google.com/billing."
      );
    }

    const by = options.by || 1;
    console.log(`[Model.increment] Increment by:`, by);

    const fieldArray = Array.isArray(fields) ? fields : [fields];
    console.log(`[Model.increment] Fields array:`, fieldArray);

    const setClauses = fieldArray
      .map((field) => `\`${field}\` = \`${field}\` + ${by}`)
      .join(", ");
    console.log(`[Model.increment] SET clauses:`, setClauses);

    console.log(`[Model.increment] Building WHERE clause`);
    const { clause: whereClause, params: whereValues } = buildWhereClause(
      options.where
    );
    console.log(`[Model.increment] WHERE clause:`, whereClause);
    console.log(`[Model.increment] WHERE values:`, whereValues);

    const sql = `UPDATE \`${this.orm.config.dataset}.${
      this.tableName
    }\` SET ${setClauses} WHERE ${whereClause || "TRUE"}`;
    console.log(`[Model.increment] Final SQL:`, sql);

    try {
      console.log(`[Model.increment] Creating query job`);
      const [job] = await this.orm.bigquery.createQueryJob({
        query: sql,
        params: whereValues,
      });
      console.log(`[Model.increment] Query job created, waiting for results`);

      await job.getQueryResults();
      console.log(`[Model.increment] Query job completed`);

      const [metadata] = await job.getMetadata();
      console.log(`[Model.increment] Job metadata retrieved`);

      const affectedRows = Number(
        metadata.statistics?.query?.numDmlAffectedRows || 0
      );
      console.log(`[Model.increment] Affected rows:`, affectedRows);

      if (this.orm.config.logging)
        console.log(
          `[Model.increment] Incremented ${affectedRows} rows in ${this.tableName}`
        );
      return affectedRows;
    } catch (err: any) {
      console.error(
        `[Model.increment] Failed to increment fields in ${this.tableName}:`,
        err.message
      );
      console.error(`[Model.increment] Failed SQL:`, sql);
      console.error(`[Model.increment] Failed params:`, whereValues);
      throw err;
    }
  }

  static async decrement(
    fields: string | string[],
    options: { by?: number; where: WhereOptions }
  ): Promise<number> {
    console.log(`[Model.decrement] Starting decrement for ${this.name}`);
    console.log(
      `[Model.decrement] Delegating to increment with negative value`
    );
    return this.increment(fields, { ...options, by: -(options.by || 1) });
  }

  private static buildSelectQuery(
    options: FindOptions,
    selectOverride?: string
  ): { sql: string; params: Record<string, any> } {
    const dataset = this.orm.config.dataset;
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
          sql += ` ${joinType} \`${dataset}.${throughTable}\` AS \`${throughAs}\` ON \`${mainAlias}\`.\`${this.primaryKey}\` = \`${throughAs}\`.\`${assoc.foreignKey}\``;
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
          const incAttributes =
            inc.attributes || Object.keys(inc.model.attributes);
          for (const field of incAttributes) {
            selectClause.push(`\`${as}\`.\`${field}\` AS \`${as}_${field}\``);
          }
        }
      }
    }

    sql = `SELECT ${selectClause.join(", ")} ${sql}`;

    if (options.group) {
      sql += ` GROUP BY ${options.group.map((g) => `\`${g}\``).join(", ")}`;
    }

    if (options.order) {
      sql += ` ORDER BY ${options.order
        .map(([field, dir]) => `\`${field}\` ${dir}`)
        .join(", ")}`;
    }

    if (options.limit) {
      sql += ` LIMIT ${options.limit}`;
    }
    if (options.offset) {
      sql += ` OFFSET ${options.offset}`;
    }

    return { sql, params };
  }

  private static nestAssociations(
    rows: any[],
    includes: IncludeOptions[]
  ): any[] {
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
            if (assoc.type === "hasMany" || assoc.type === "belongsToMany") {
              parent[as] = [];
            } else {
              parent[as] = null;
            }
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
        for (const field in inc.model.attributes) {
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

    return Array.from(parentMap.values());
  }
}
