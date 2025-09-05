// src/dataTypes.ts
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

export type DataType = DataTypeAttribute;

export interface DataTypes {
  STRING: (options?: Partial<DataTypeAttribute>) => DataType;
  CHAR: (options?: Partial<DataTypeAttribute>) => DataType;
  TEXT: (options?: Partial<DataTypeAttribute>) => DataType;
  INTEGER: (options?: Partial<DataTypeAttribute>) => DataType;
  TINYINT: (options?: Partial<DataTypeAttribute>) => DataType;
  SMALLINT: (options?: Partial<DataTypeAttribute>) => DataType;
  MEDIUMINT: (options?: Partial<DataTypeAttribute>) => DataType;
  BIGINT: (options?: Partial<DataTypeAttribute>) => DataType;
  FLOAT: (options?: Partial<DataTypeAttribute>) => DataType;
  DOUBLE: (options?: Partial<DataTypeAttribute>) => DataType;
  DECIMAL: (
    precision: number,
    scale: number,
    options?: Partial<DataTypeAttribute>
  ) => DataType;
  BOOLEAN: (options?: Partial<DataTypeAttribute>) => DataType;
  DATE: (options?: Partial<DataTypeAttribute>) => DataType;
  DATEONLY: (options?: Partial<DataTypeAttribute>) => DataType;
  TIME: (options?: Partial<DataTypeAttribute>) => DataType;
  DATETIME: (options?: Partial<DataTypeAttribute>) => DataType;
  JSON: (options?: Partial<DataTypeAttribute>) => DataType;
  JSONB: (options?: Partial<DataTypeAttribute>) => DataType;
  BLOB: (options?: Partial<DataTypeAttribute>) => DataType;
  UUID: (options?: Partial<DataTypeAttribute>) => DataType;
  ARRAY: (itemType: DataType) => DataType;
  STRUCT: (
    fields: Record<string, DataType>,
    options?: Partial<DataTypeAttribute>
  ) => DataType;
  GEOGRAPHY: (options?: Partial<DataTypeAttribute>) => DataType;
  INTERVAL: (options?: Partial<DataTypeAttribute>) => DataType;
  BYTES: (options?: Partial<DataTypeAttribute>) => DataType;
  NOW: string;
  NOW_DATETIME: string;
  UUIDV4: string;
}

export const DataTypes: DataTypes = {
  STRING: (options = {}) => ({ type: "STRING", allowNull: true, ...options }),
  CHAR: (options = {}) => ({ type: "STRING", allowNull: true, ...options }),
  TEXT: (options = {}) => ({ type: "STRING", allowNull: true, ...options }),
  INTEGER: (options = {}) => ({ type: "INT64", allowNull: true, ...options }),
  TINYINT: (options = {}) => ({ type: "INT64", allowNull: true, ...options }),
  SMALLINT: (options = {}) => ({ type: "INT64", allowNull: true, ...options }),
  MEDIUMINT: (options = {}) => ({ type: "INT64", allowNull: true, ...options }),
  BIGINT: (options = {}) => ({ type: "INT64", allowNull: true, ...options }),
  FLOAT: (options = {}) => ({ type: "FLOAT64", allowNull: true, ...options }),
  DOUBLE: (options = {}) => ({ type: "FLOAT64", allowNull: true, ...options }),
  DECIMAL: (precision: number, scale: number, options = {}) => ({
    type: "NUMERIC",
    precision,
    scale,
    allowNull: true,
    ...options,
  }),
  BOOLEAN: (options = {}) => ({ type: "BOOL", allowNull: true, ...options }),
  DATE: (options = {}) => ({ type: "TIMESTAMP", allowNull: true, ...options }),
  DATEONLY: (options = {}) => ({ type: "DATE", allowNull: true, ...options }),
  TIME: (options = {}) => ({ type: "TIME", allowNull: true, ...options }),
  DATETIME: (options = {}) => ({
    type: "DATETIME",
    allowNull: true,
    ...options,
  }),
  JSON: (options = {}) => ({ type: "JSON", allowNull: true, ...options }),
  JSONB: (options = {}) => ({ type: "JSON", allowNull: true, ...options }),
  BLOB: (options = {}) => ({ type: "BYTES", allowNull: true, ...options }),
  UUID: (options = {}) => ({ type: "STRING", allowNull: true, ...options }),
  ARRAY: (itemType: DataType) => ({ ...itemType, mode: "REPEATED" }),
  STRUCT: (fields: Record<string, DataType>, options = {}) => ({
    type: "STRUCT",
    fields,
    allowNull: true,
    ...options,
  }),
  GEOGRAPHY: (options = {}) => ({
    type: "GEOGRAPHY",
    allowNull: true,
    ...options,
  }),
  INTERVAL: (options = {}) => ({
    type: "INTERVAL",
    allowNull: true,
    ...options,
  }),
  BYTES: (options = {}) => ({ type: "BYTES", allowNull: true, ...options }),

  // Default value helpers
  NOW: "CURRENT_TIMESTAMP()", // For TIMESTAMP
  NOW_DATETIME: "CURRENT_DATETIME()", // For DATETIME
  UUIDV4: "GENERATE_UUID()",
};
