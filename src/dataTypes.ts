import * as crypto from "crypto";

export interface DataTypeAttribute {
  type: string;
  allowNull?: boolean;
  defaultValue?: any;
  primaryKey?: boolean;
  mode?: "REPEATED";
  fields?: Record<string, DataTypeAttribute>;
  precision?: number;
  scale?: number;
}

export const DataTypes = {
  STRING: (options: any = {}) => ({
    type: "STRING",
    allowNull: true,
    ...options,
  }),
  CHAR: (options: any = {}) => ({
    type: "STRING",
    allowNull: true,
    ...options,
  }),
  TEXT: (options: any = {}) => ({
    type: "STRING",
    allowNull: true,
    ...options,
  }),
  INTEGER: (options: any = {}) => ({
    type: "INT64",
    allowNull: true,
    ...options,
  }),
  TINYINT: (options: any = {}) => ({
    type: "INT64",
    allowNull: true,
    ...options,
  }),
  SMALLINT: (options: any = {}) => ({
    type: "INT64",
    allowNull: true,
    ...options,
  }),
  MEDIUMINT: (options: any = {}) => ({
    type: "INT64",
    allowNull: true,
    ...options,
  }),
  BIGINT: (options: any = {}) => ({
    type: "INT64",
    allowNull: true,
    ...options,
  }),
  FLOAT: (options: any = {}) => ({
    type: "FLOAT64",
    allowNull: true,
    ...options,
  }),
  DOUBLE: (options: any = {}) => ({
    type: "FLOAT64",
    allowNull: true,
    ...options,
  }),
  DECIMAL: (precision: number, scale: number, options: any = {}) => ({
    type: "NUMERIC",
    precision,
    scale,
    allowNull: true,
    ...options,
  }),
  BOOLEAN: (options: any = {}) => ({
    type: "BOOL",
    allowNull: true,
    ...options,
  }),
  DATE: (options: any = {}) => ({
    type: "TIMESTAMP",
    allowNull: true,
    ...options,
  }),
  DATEONLY: (options: any = {}) => ({
    type: "DATE",
    allowNull: true,
    ...options,
  }),
  TIME: (options: any = {}) => ({ type: "TIME", allowNull: true, ...options }),
  DATETIME: (options: any = {}) => ({
    type: "DATETIME",
    allowNull: true,
    ...options,
  }),
  JSON: (options: any = {}) => ({ type: "JSON", allowNull: true, ...options }),
  JSONB: (options: any = {}) => ({ type: "JSON", allowNull: true, ...options }),
  BLOB: (options: any = {}) => ({ type: "BYTES", allowNull: true, ...options }),
  UUID: (options: any = {}) => ({
    type: "STRING",
    allowNull: true,
    ...options,
  }),
  ARRAY: (itemType: DataTypeAttribute) => ({ ...itemType, mode: "REPEATED" }),
  STRUCT: (fields: Record<string, DataTypeAttribute>, options: any = {}) => ({
    type: "STRUCT",
    fields,
    allowNull: true,
    ...options,
  }),
  GEOGRAPHY: (options: any = {}) => ({
    type: "GEOGRAPHY",
    allowNull: true,
    ...options,
  }),
  INTERVAL: (options: any = {}) => ({
    type: "INTERVAL",
    allowNull: true,
    ...options,
  }),
  BYTES: (options: any = {}) => ({
    type: "BYTES",
    allowNull: true,
    ...options,
  }),
  NOW: "CURRENT_TIMESTAMP()",
  UUIDV4: "GENERATE_UUID()",
};

export type DataType = DataTypeAttribute;
