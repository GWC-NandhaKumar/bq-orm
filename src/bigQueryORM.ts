// src/bigQueryORM.ts
import { BigQuery } from "@google-cloud/bigquery";
import * as fs from "fs";
import * as path from "path";
import { Model } from "./model";
import { DataType, DataTypes } from "./dataTypes";
import { QueryInterface } from "./queryInterface";
import { dataTypeToSchemaField } from "./utils";
import { createLogger, Logger } from "./logger";
import * as readline from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";

export interface BigQueryORMConfig {
  projectId: string;
  dataset: string;
  keyFilename?: string;
  logging?: boolean;
  freeTierMode?: boolean;
}

export class BigQueryORM {
  public bigquery: BigQuery;
  public config: Required<BigQueryORMConfig>;
  public models: Record<string, typeof Model> = {};
  private queryInterface: QueryInterface;
  private executedMigrations: Set<string> = new Set();
  public logger: Logger;

  constructor(config?: Partial<BigQueryORMConfig>) {
    // Default logging to false if undefined
    const logging =
      config?.logging ?? process.env.BIGQUERY_ORM_LOGGING === "true";
    this.logger = createLogger(logging);
    this.logger.info("[BigQueryORM:constructor] Initializing BigQueryORM", {
      config,
    });

    this.config = {
      projectId: config?.projectId || process.env.GOOGLE_CLOUD_PROJECT || "",
      dataset:
        config?.dataset || process.env.BIGQUERY_DATASET || "default_dataset",
      keyFilename:
        config?.keyFilename || process.env.GOOGLE_APPLICATION_CREDENTIALS || "",
      logging,
      freeTierMode: config?.freeTierMode ?? false,
    };

    if (!this.config.projectId) {
      this.logger.error(
        "[BigQueryORM:constructor] projectId must be provided via config or GOOGLE_CLOUD_PROJECT env"
      );
      throw new Error(
        "projectId must be provided via config or GOOGLE_CLOUD_PROJECT env."
      );
    }

    this.bigquery = new BigQuery({
      projectId: this.config.projectId,
      keyFilename: this.config.keyFilename,
    });
    this.queryInterface = new QueryInterface(this);
    this.logger.info(
      "[BigQueryORM:constructor] BigQueryORM initialized successfully"
    );
  }

  async createDataset(
    options: { location?: string; labels?: Record<string, string> } = {}
  ): Promise<void> {
    this.logger.info("[BigQueryORM:createDataset] Starting dataset creation", {
      options,
    });
    if (!this.config.dataset || this.config.dataset === "default_dataset") {
      this.logger.info(
        "[BigQueryORM:createDataset] No dataset name provided or default used; prompting for input"
      );
      const rl = readline.createInterface({ input, output });
      this.config.dataset = await rl.question(
        "Enter the dataset name to create: "
      );
      rl.close();
      this.logger.info(
        "[BigQueryORM:createDataset] Dataset name set from prompt",
        { dataset: this.config.dataset }
      );
    }

    const dataset = this.bigquery.dataset(this.config.dataset);
    const [dsExists] = await dataset.exists();
    if (dsExists) {
      this.logger.info(
        `[BigQueryORM:createDataset] Dataset ${this.config.dataset} already exists, skipping creation`
      );
      return;
    }

    try {
      await dataset.create({
        location: options.location,
        labels: options.labels,
      });
      this.logger.info(
        `[BigQueryORM:createDataset] Created dataset ${this.config.dataset}`,
        options
      );
    } catch (err: any) {
      this.logger.error(
        `[BigQueryORM:createDataset] Failed to create dataset ${this.config.dataset}:`,
        err.message
      );
      throw err;
    }
  }

  async authenticate(): Promise<void> {
    this.logger.info("[BigQueryORM:authenticate] Starting authentication");
    if (this.config.freeTierMode) {
      this.logger.warn(
        "[BigQueryORM:authenticate] Free tier mode: Limited to SELECT queries within 1TB limit."
      );
    }
    try {
      await this.bigquery.getDatasets({ maxResults: 1 });
      this.logger.info("[BigQueryORM:authenticate] Authentication successful");
    } catch (err: any) {
      this.logger.error(
        "[BigQueryORM:authenticate] Authentication failed:",
        err.message
      );
      throw err;
    }
  }

  define(
    name: string,
    attributes: Record<string, DataType>,
    options: { tableName?: string; primaryKey?: string } = {}
  ): typeof Model {
    this.logger.info("[BigQueryORM:define] Defining model", {
      name,
      attributes: Object.keys(attributes),
      options,
    });
    class DynamicModel extends Model {}
    DynamicModel.init(attributes, {
      orm: this,
      tableName: options.tableName,
      primaryKey: options.primaryKey,
    });
    this.models[name] = DynamicModel;
    this.logger.info("[BigQueryORM:define] Model defined successfully", {
      name,
    });
    return DynamicModel;
  }

  async loadModels(modelsPath: string): Promise<void> {
    this.logger.info(
      "[BigQueryORM:loadModels] Starting to load models from path",
      { modelsPath }
    );
    try {
      const files = fs
        .readdirSync(modelsPath)
        .filter(
          (f) =>
            !f.endsWith(".d.ts") && (f.endsWith(".ts") || f.endsWith(".js"))
        );
      this.logger.info("[BigQueryORM:loadModels] Found files", { files });

      for (const file of files) {
        const modelFunc = (await import(path.resolve(modelsPath, file)))
          .default;
        if (typeof modelFunc === "function") {
          modelFunc(this, DataTypes);
          this.logger.info("[BigQueryORM:loadModels] Loaded model from file", {
            file,
          });
        } else {
          this.logger.warn(
            "[BigQueryORM:loadModels] Invalid model file, expected a function",
            { file }
          );
        }
      }

      for (const model of Object.values(this.models)) {
        if (typeof (model as any).associate === "function") {
          (model as any).associate(this.models);
          this.logger.info("[BigQueryORM:loadModels] Associated model", {
            modelName: model.name,
          });
        } else {
          this.logger.info(
            "[BigQueryORM:loadModels] No associate function for model",
            { modelName: model.name }
          );
        }
      }
      this.logger.info(
        "[BigQueryORM:loadModels] All models loaded and associated"
      );
    } catch (err: any) {
      this.logger.error(
        "[BigQueryORM:loadModels] Failed to load models:",
        err.message
      );
      throw err;
    }
  }

  async sync(
    options: { force?: boolean; alter?: boolean } = {}
  ): Promise<void> {
    this.logger.info("[BigQueryORM:sync] Starting sync", { options });
    const { force = false, alter = false } = options;
    if (this.config.freeTierMode && (force || alter)) {
      this.logger.warn(
        "[BigQueryORM:sync] Free tier mode: Table creation/deletion may incur storage costs."
      );
    }

    const dataset = this.bigquery.dataset(this.config.dataset);
    const [dsExists] = await dataset.exists();
    if (!dsExists) {
      await dataset.create();
      this.logger.info(
        `[BigQueryORM:sync] Created dataset ${this.config.dataset}`
      );
    }

    for (const model of Object.values(this.models)) {
      const table = dataset.table(model.tableName);
      const [tExists] = await table.exists();
      if (tExists && force) {
        await table.delete();
        this.logger.info(`[BigQueryORM:sync] Deleted table ${model.tableName}`);
      }
      if (!tExists || force) {
        const schema = Object.entries(model.attributes).map(([name, type]) =>
          dataTypeToSchemaField(name, type)
        );
        await table.create({ schema });
        this.logger.info(`[BigQueryORM:sync] Created table ${model.tableName}`);
      } else if (alter) {
        this.logger.warn(
          "[BigQueryORM:sync] Alter sync not supported in free tier; manual migration recommended."
        );
      }
    }
  }

  getQueryInterface(): QueryInterface {
    this.logger.info(
      "[BigQueryORM:getQueryInterface] Returning query interface"
    );
    return this.queryInterface;
  }

  async runMigrations(migrationsPath: string): Promise<void> {
    this.logger.info("[BigQueryORM:runMigrations] Starting migrations", {
      migrationsPath,
    });
    if (this.config.freeTierMode) {
      this.logger.warn(
        "[BigQueryORM:runMigrations] Free tier mode: Migrations use in-memory tracking."
      );
    }

    const dataset = this.bigquery.dataset(this.config.dataset);
    let [dsExists] = await dataset.exists();
    if (!dsExists) {
      await dataset.create();
      this.logger.info(
        `[BigQueryORM:runMigrations] Created dataset ${this.config.dataset}`
      );
    }

    const metaTable = dataset.table("migrations");
    let [tExists] = await metaTable.exists();
    if (!tExists && !this.config.freeTierMode) {
      await metaTable.create({
        schema: [
          { name: "name", type: "STRING" },
          { name: "executed_at", type: "TIMESTAMP" },
        ],
      });
      this.logger.info("[BigQueryORM:runMigrations] Created migrations table");
    }

    let executed: Set<string>;
    if (this.config.freeTierMode) {
      executed = this.executedMigrations;
      this.logger.info(
        "[BigQueryORM:runMigrations] Using in-memory executed migrations for free tier"
      );
    } else {
      const [rows] = await this.bigquery.query({
        query: `SELECT name FROM \`${this.config.projectId}.${this.config.dataset}.migrations\` ORDER BY executed_at ASC`,
      });
      executed = new Set(rows.map((r: any) => r.name));
      this.logger.info(
        "[BigQueryORM:runMigrations] Fetched executed migrations",
        { count: executed.size }
      );
    }

    const migrationFiles = fs
      .readdirSync(migrationsPath)
      .filter(
        (f) => !f.endsWith(".d.ts") && (f.endsWith(".ts") || f.endsWith(".js"))
      )
      .sort();
    this.logger.info("[BigQueryORM:runMigrations] Found migration files", {
      files: migrationFiles,
    });

    for (const file of migrationFiles) {
      const migrationName = path.basename(file, path.extname(file));
      if (executed.has(migrationName)) {
        this.logger.info(
          `[BigQueryORM:runMigrations] Skipping migration ${migrationName} (already executed)`
        );
        continue;
      }

      const migrationModule = await import(path.resolve(migrationsPath, file));
      const migration = migrationModule.default || migrationModule;
      await migration.up(this.queryInterface, this);
      if (this.config.freeTierMode) {
        this.executedMigrations.add(migrationName);
        this.logger.info(
          `[BigQueryORM:runMigrations] Migration ${migrationName} tracked in-memory`
        );
      } else {
        const sql = `INSERT INTO \`${this.config.projectId}.${this.config.dataset}.migrations\` (name, executed_at) VALUES (@name, @executed_at)`;
        await this.bigquery.query({
          query: sql,
          params: {
            name: migrationName,
            executed_at: new Date().toISOString(),
          },
        });
        this.logger.info(
          `[BigQueryORM:runMigrations] Migration ${migrationName} executed and recorded`
        );
      }
    }
  }

  async revertLastMigration(migrationsPath: string): Promise<void> {
    this.logger.info(
      "[BigQueryORM:revertLastMigration] Starting revert last migration",
      { migrationsPath }
    );
    if (this.config.freeTierMode) {
      this.logger.warn(
        "[BigQueryORM:revertLastMigration] Free tier mode: Reverting migrations not supported."
      );
      return;
    }

    const metaTable = this.bigquery
      .dataset(this.config.dataset)
      .table("migrations");
    const [rows] = await this.bigquery.query({
      query: `SELECT name FROM \`${this.config.projectId}.${this.config.dataset}.migrations\` ORDER BY executed_at DESC LIMIT 1`,
    });
    if (!rows.length) {
      this.logger.info(
        "[BigQueryORM:revertLastMigration] No migrations to revert"
      );
      return;
    }
    const migrationName = rows[0].name;

    const migrationFiles = fs
      .readdirSync(migrationsPath)
      .filter(
        (f) => !f.endsWith(".d.ts") && (f.endsWith(".ts") || f.endsWith(".js"))
      );
    const migrationFile = migrationFiles.find(
      (f) => path.basename(f, path.extname(f)) === migrationName
    );
    if (!migrationFile) {
      this.logger.error(
        "[BigQueryORM:revertLastMigration] Migration file not found",
        { migrationName }
      );
      throw new Error(`Migration file not found: ${migrationName}`);
    }

    const migrationModule = await import(
      path.resolve(migrationsPath, migrationFile)
    );
    const migration = migrationModule.default || migrationModule;
    await migration.down(this.queryInterface, this);
    const sql = `DELETE FROM \`${this.config.projectId}.${this.config.dataset}.migrations\` WHERE name = @migrationName`;
    await this.bigquery.query({ query: sql, params: { migrationName } });
    this.logger.info(
      `[BigQueryORM:revertLastMigration] Reverted migration ${migrationName}`
    );
  }

  async transaction(fn: (qi: QueryInterface) => Promise<void>): Promise<void> {
    this.logger.info("[BigQueryORM:transaction] Starting transaction");
    if (this.config.freeTierMode) {
      this.logger.warn(
        "[BigQueryORM:transaction] Free tier mode: Transactions limited to SELECT queries."
      );
    }
    try {
      await fn(this.queryInterface);
      this.logger.info("[BigQueryORM:transaction] Transaction successful");
    } catch (err: any) {
      this.logger.error(
        "[BigQueryORM:transaction] Transaction failed:",
        err.message
      );
      throw err;
    }
  }
}
