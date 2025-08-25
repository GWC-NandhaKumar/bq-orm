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

  static async findAll(options: FindOptions = {}): Promise<any[]> {
    const { sql, params } = this.buildSelectQuery(options);
    this.orm.logger.info(`[Model:findAll] Executing query for ${this.name}`, {
      sql,
      params,
    });
    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    const result = options.raw
      ? rows
      : this.nestAssociations(rows, options.include || []);
    this.orm.logger.info(
      `[Model:findAll] Found ${result.length} records for ${this.name}`
    );
    return result;
  }

  static async findOne(options: FindOptions = {}): Promise<any | null> {
    this.orm.logger.info(
      `[Model:findOne] Finding one record for ${this.name}`,
      { options }
    );
    const results = await this.findAll({ ...options, limit: 1 });
    const result = results[0] || null;
    this.orm.logger.info(
      `[Model:findOne] Found record: ${result ? "yes" : "no"} for ${this.name}`
    );
    return result;
  }

  static async findByPk(
    pk: any,
    options: FindOptions = {}
  ): Promise<any | null> {
    this.orm.logger.info(`[Model:findByPk] Finding by PK for ${this.name}`, {
      pk,
      options,
    });
    return this.findOne({ ...options, where: { [this.primaryKey]: pk } });
  }

  static async count(options: FindOptions = {}): Promise<number> {
    this.orm.logger.info(`[Model:count] Counting records for ${this.name}`, {
      options,
    });
    const select = `COUNT(DISTINCT \`${this.tableName}\`.\`${this.primaryKey}\`) AS count`;
    const { sql, params } = this.buildSelectQuery(options, select);
    const [rows] = await this.orm.bigquery.query({ query: sql, params });
    const count = rows[0]?.count || 0;
    this.orm.logger.info(
      `[Model:count] Counted ${count} records for ${this.name}`
    );
    return count;
  }

  private static resolveDefault(value: any): any {
    if (value === DataTypes.NOW || value === "CURRENT_TIMESTAMP()") {
      return new Date();
    } else if (value === DataTypes.UUIDV4 || value === "GENERATE_UUID()") {
      return crypto.randomUUID();
    }
    return value;
  }

  static async create(data: Record<string, any>): Promise<any> {
    this.orm.logger.info(`[Model:create] Creating record for ${this.name}`, {
      data,
    });
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
          `[Model:create] Missing required field ${field} for ${this.name}`
        );
        throw new Error(`Missing required field ${field}`);
      }
    }
    const table = this.orm.bigquery
      .dataset(this.orm.config.dataset)
      .table(this.tableName);
    await table.insert([filledData]);
    this.orm.logger.info(`[Model:create] Record created for ${this.name}`);
    return filledData;
  }

  static async bulkCreate(data: Record<string, any>[]): Promise<void> {
    this.orm.logger.info(
      `[Model:bulkCreate] Creating ${data.length} records for ${this.name}`
    );
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[Model:bulkCreate] Free tier mode: BULK CREATE (INSERT) not allowed."
      );
      throw new Error("Free tier mode: BULK CREATE (INSERT) not allowed.");
    }
    if (!data.length) {
      this.orm.logger.info("[Model:bulkCreate] No records to create, skipping");
      return;
    }
    const filledData = data.map((record) => {
      const filled: Record<string, any> = {};
      for (const [field, attr] of Object.entries(this.attributes)) {
        if (field in record) {
          filled[field] = record[field];
        } else if (attr.defaultValue !== undefined) {
          filled[field] = this.resolveDefault(attr.defaultValue);
        } else if (attr.allowNull === false) {
          this.orm.logger.error(
            `[Model:bulkCreate] Missing required field ${field} in bulk create record for ${this.name}`
          );
          throw new Error(
            `Missing required field ${field} in bulk create record`
          );
        }
      }
      return filled;
    });
    const table = this.orm.bigquery
      .dataset(this.orm.config.dataset)
      .table(this.tableName);
    await table.insert(filledData);
    this.orm.logger.info(
      `[Model:bulkCreate] ${filledData.length} records created for ${this.name}`
    );
  }

  static async update(
    data: Record<string, any>,
    options: { where: WhereOptions }
  ): Promise<number> {
    this.orm.logger.info(`[Model:update] Updating records for ${this.name}`, {
      data,
      options,
    });
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
    const sql = `UPDATE \`${this.orm.config.dataset}.${
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
      `[Model:update] Updated ${affectedRows} records for ${this.name}`
    );
    return affectedRows;
  }

  static async destroy(options: { where: WhereOptions }): Promise<number> {
    this.orm.logger.info(`[Model:destroy] Deleting records for ${this.name}`, {
      options,
    });
    if (this.orm.config.freeTierMode) {
      this.orm.logger.error(
        "[Model:destroy] Free tier mode: DESTROY (DELETE) not allowed."
      );
      throw new Error("Free tier mode: DESTROY (DELETE) not allowed.");
    }
    const { clause, params } = buildWhereClause(options.where);
    const sql = `DELETE FROM \`${this.orm.config.dataset}.${
      this.tableName
    }\` WHERE ${clause || "TRUE"}`;
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
      `[Model:destroy] Deleted ${affectedRows} records for ${this.name}`
    );
    return affectedRows;
  }

  static async increment(
    fields: string | string[],
    options: { by?: number; where: WhereOptions }
  ): Promise<number> {
    this.orm.logger.info(
      `[Model:increment] Incrementing fields for ${this.name}`,
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
    const sql = `UPDATE \`${this.orm.config.dataset}.${
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
      `[Model:increment] Incremented ${affectedRows} records for ${this.name}`
    );
    return affectedRows;
  }

  static async decrement(
    fields: string | string[],
    options: { by?: number; where: WhereOptions }
  ): Promise<number> {
    this.orm.logger.info(
      `[Model:decrement] Decrementing fields for ${this.name}`,
      { fields, options }
    );
    return this.increment(fields, { ...options, by: -(options.by || 1) });
  }

  private static buildSelectQuery(
    options: FindOptions,
    selectOverride?: string
  ): { sql: string; params: Record<string, any> } {
    this.orm.logger.info(
      `[Model:buildSelectQuery] Building query for ${this.name}`,
      { options, selectOverride }
    );
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
        if (!assoc) {
          this.orm.logger.error(
            `[Model:buildSelectQuery] Association not found for ${inc.model.name} in ${this.name}`
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
              `[Model:buildSelectQuery] Through model and otherKey required for belongsToMany in ${this.name}`
            );
            throw new Error(
              "Through model and otherKey required for belongsToMany"
            );
          }
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

    this.orm.logger.info(
      `[Model:buildSelectQuery] Generated SQL for ${this.name}`,
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

    const result = Array.from(parentMap.values());
    this.orm.logger.info(
      `[Model:nestAssociations] Nested ${result.length} records for ${this.name}`
    );
    return result;
  }
}
