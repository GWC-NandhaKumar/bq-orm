# BigQuery ORM (`bg-orm`)

`bg-orm` is a comprehensive, Sequelize-inspired Object-Relational Mapping (ORM) library tailored for Google BigQuery. It enables Node.js developers to interact with BigQuery databases using familiar ORM patterns, including model definitions, associations, CRUD operations, migrations, and advanced querying. The library supports BigQuery's unique features such as partitioning, clustering, structured data types (e.g., `STRUCT`, `ARRAY`, `JSON`), and free-tier limitations.

This README provides an in-depth guide for developers, covering all aspects of the library based on a thorough analysis of the source code. It includes detailed explanations of data types, model methods, relationships, migration files, model files, and usage examples.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Defining Models](#defining-models)
  - [Model Initialization](#model-initialization)
  - [Model Files Structure](#model-files-structure)
- [All Supported Data Types](#all-supported-data-types)
  - [Using JSON Data Type](#using-json-data-type)
  - [Using ARRAY Data Type](#using-array-data-type)
  - [Using STRUCT Data Type](#using-struct-data-type)
- [Relationships (Associations)](#relationships-associations)
  - [belongsTo](#belongsto)
  - [hasOne](#hasone)
  - [hasMany](#hasmany)
  - [belongsToMany](#belongstomany)
- [Model Methods](#model-methods)
  - [Query Methods](#query-methods)
  - [CRUD Methods](#crud-methods)
  - [Utility Methods](#utility-methods)
- [Query Options](#query-options)
- [CRUD Operations](#crud-operations)
- [Query Interface](#query-interface)
- [Migrations](#migrations)
  - [Migration Files Structure](#migration-files-structure)
  - [Running Migrations](#running-migrations)
- [Free Tier Mode](#free-tier-mode)
- [Operators (Op)](#operators-op)
- [Syncing Schema](#syncing-schema)
- [Transactions](#transactions)
- [Loading Models](#loading-models)
- [Error Handling](#error-handling)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Model Definition & Attributes**: Define models with precise control over attributes, including defaults, nullability, and primary keys.
- **All Data Types Supported**: Comprehensive mapping to BigQuery data types, including advanced ones like `ARRAY`, `STRUCT`, `JSON`, `GEOGRAPHY`, and `INTERVAL`.
- **Associations**: Full support for one-to-one, one-to-many, and many-to-many relationships with automatic JOIN handling and eager loading.
- **Model Methods**: Extensive static methods for querying (e.g., `findAll`, `count`), CRUD (e.g., `create`, `update`, `destroy`), and utilities (e.g., `increment`).
- **Migrations**: Scriptable up/down migrations for schema management, with support for free-tier in-memory tracking.
- **Query Building**: Advanced query options for `WHERE`, `INCLUDE`, `ORDER`, `GROUP`, `LIMIT`, and more.
- **Free Tier Compatibility**: Restricts DML operations to stay within BigQuery's free limits.
- **TypeScript-First**: Strongly typed interfaces for models, options, and data types.

## Installation

```bash
npm install bg-orm
npm install @google-cloud/bigquery  # Peer dependency
```

## Configuration

```javascript
import { BigQueryORM } from "bg-orm";

const orm = new BigQueryORM({
  projectId: "your-project-id", // Required
  dataset: "your-dataset", // Required
  keyFilename: "/path/to/key.json", // Optional
  logging: true, // Optional: Enable console logs
  freeTierMode: true, // Optional: Enforce free-tier restrictions
});

await orm.authenticate(); // Throws if authentication fails
```

Use environment variables for convenience:

- `GOOGLE_CLOUD_PROJECT`
- `BIGQUERY_DATASET`
- `GOOGLE_APPLICATION_CREDENTIALS`

## Defining Models

Models are abstract classes extending `Model`. Use `orm.define` or export functions in model files.

### Model Initialization

From the code, models are initialized with:

```javascript
static init(attributes: Record<string, DataType>, options: { orm: BigQueryORM; tableName?: string; primaryKey?: string })
```

- Sets `attributes`, `tableName`, `primaryKey`, and `orm`.
- Default `primaryKey` is 'id' or the first primaryKey attribute.

Example:

```javascript
const User = orm.define("User", {
  id: DataTypes.INTEGER({ primaryKey: true, allowNull: false }),
  name: DataTypes.STRING({ allowNull: false }),
});
```

### Model Files Structure

Model files are typically placed in a `/models` directory and exported as functions that define the model and associations.

Example `/models/user.ts`:

```typescript
import { DataTypes } from "bg-orm"; // Assuming relative import

export default (orm, DataTypes) => {
  const User = orm.define(
    "User",
    {
      id: DataTypes.INTEGER({ primaryKey: true, allowNull: false }),
      name: DataTypes.STRING({ allowNull: false }),
      email: DataTypes.STRING({ allowNull: false }),
      age: DataTypes.INTEGER(),
      preferences: DataTypes.JSON(),
      tags: DataTypes.ARRAY(DataTypes.STRING()),
      address: DataTypes.STRUCT({
        street: DataTypes.STRING(),
        city: DataTypes.STRING(),
      }),
      createdAt: DataTypes.DATETIME({ defaultValue: DataTypes.NOW }),
      uuid: DataTypes.UUID({ defaultValue: DataTypes.UUIDV4 }),
    },
    { tableName: "users", primaryKey: "id" }
  );

  User.associate = (models) => {
    User.hasMany(models.Order, { foreignKey: "userId", as: "orders" });
    User.belongsTo(models.Group, { foreignKey: "groupId", as: "group" });
  };

  return User;
};
```

Load all models:

```javascript
await orm.loadModels("./models"); // Loads and initializes associations
```

## All Supported Data Types

Based on the `DataTypes` export in `dataTypes.ts`, here is a complete list with mappings and usage:

- **STRING()**: BigQuery `STRING`. For text/variable-length strings.
- **CHAR()**: Alias for `STRING`.
- **TEXT()**: Alias for `STRING`.
- **INTEGER()**: BigQuery `INT64`. For integers.
- **TINYINT()**, **SMALLINT()**, **MEDIUMINT()**, **BIGINT()**: Aliases for `INT64`.
- **FLOAT()**: BigQuery `FLOAT64`. For floating-point numbers.
- **DOUBLE()**: Alias for `FLOAT64`.
- **DECIMAL(precision: number, scale: number)**: BigQuery `NUMERIC` or `BIGNUMERIC`. E.g., `DECIMAL(10, 2)` for currency.
- **BOOLEAN()**: BigQuery `BOOL`.
- **DATE()**: BigQuery `DATE`.
- **DATEONLY()**: Alias for `DATE`.
- **TIME()**: BigQuery `TIME`.
- **DATETIME()**: BigQuery `DATETIME`.
- **JSON()**: BigQuery `JSON`. For semi-structured data.
- **JSONB()**: Alias for `JSON`.
- **BLOB()**: BigQuery `BYTES`. For binary data.
- **BYTES()**: Alias for `BYTES`.
- **UUID()**: BigQuery `STRING`. Use with `defaultValue: DataTypes.UUIDV4`.
- **ARRAY(itemType: DataType)**: BigQuery `ARRAY`. E.g., `ARRAY(DataTypes.INTEGER())`.
- **STRUCT(fields: Record<string, DataType>)**: BigQuery `STRUCT`. E.g., `STRUCT({ x: DataTypes.INTEGER() })`.
- **GEOGRAPHY()**: BigQuery `GEOGRAPHY`. For spatial data.
- **INTERVAL()**: BigQuery `INTERVAL`. For time durations.
- **NOW**: Default value `CURRENT_TIMESTAMP()`.
- **UUIDV4**: Default value `GENERATE_UUID()`.

Options for all types (where applicable):

- `allowNull?: boolean` (default: true)
- `defaultValue?: any` (e.g., `DataTypes.NOW`, static value)
- `primaryKey?: boolean`
- `mode?: 'REPEATED'` (for arrays)
- `fields?: Record<string, DataType>` (for STRUCT)
- `precision?: number`, `scale?: number` (for DECIMAL/NUMERIC)

### Using JSON Data Type

Store JSON objects/arrays. Query using BigQuery JSON functions in raw queries.

Example:

```javascript
const user = await User.create({ preferences: { theme: "dark", lang: "en" } });

// Raw query to extract
const [rows] = await orm.bigquery.query(
  `SELECT JSON_VALUE(preferences, '$.theme') FROM users`
);
```

### Using ARRAY Data Type

Store lists. Use `UNNEST` for querying.

Example:

```javascript
const post = await Post.create({ tags: ["js", "orm"] });

// Raw query
const [rows] = await orm.bigquery.query(
  `SELECT * FROM posts WHERE 'js' IN UNNEST(tags)`
);
```

### Using STRUCT Data Type

Nested records.

Example:

```javascript
const user = await User.create({ address: { street: "123 Main", city: "NY" } });

// Query
const [rows] = await orm.bigquery.query(`SELECT address.city FROM users`);
```

## Relationships (Associations)

Associations are stored in `static associations: Record<string, Association>`.

- `type`: 'hasOne' | 'hasMany' | 'belongsTo' | 'belongsToMany'
- `target`: Target model
- `foreignKey`: Foreign key field
- `otherKey?`: For belongsToMany
- `as?`: Alias
- `through?`: Junction model for belongsToMany

### belongsTo

Adds foreignKey to source model.

```javascript
Order.belongsTo(User, { foreignKey: "userId", as: "user" });
```

### hasOne

Target has foreignKey referencing source.

```javascript
User.hasOne(Profile, { foreignKey: "userId", as: "profile" });
```

### hasMany

Target has foreignKey, results in array.

```javascript
User.hasMany(Order, { foreignKey: "userId", as: "orders" });
```

### belongsToMany

Uses through model.

```javascript
Post.belongsToMany(Tag, {
  through: PostTag,
  foreignKey: "postId",
  otherKey: "tagId",
  as: "tags",
});
```

Eager load with `include` in find options.

## Model Methods

All static methods from `model.ts`:

### Query Methods

- **findAll(options: FindOptions = {})**: Returns array of records. Supports raw: true for flat results.
- **findOne(options: FindOptions = {})**: Returns first record or null. Internally calls findAll with limit 1.
- **findByPk(pk: any, options: FindOptions = {})**: Finds by primary key.
- **count(options: FindOptions = {})**: Returns count of distinct primary keys.

### CRUD Methods

- **create(data: Record<string, any>)**: Inserts one record, resolves defaults (NOW, UUIDV4).
- **bulkCreate(data: Record<string, any>[]) **: Inserts multiple, fills defaults.
- **update(data: Record<string, any>, options: { where: WhereOptions })**: Updates matching records, returns affected rows.
- **destroy(options: { where: WhereOptions })**: Deletes matching records, returns affected rows.

### Utility Methods

- **increment(fields: string | string[], options: { by?: number; where: WhereOptions })**: Increments fields.
- **decrement(fields: string | string[], options: { by?: number; where: WhereOptions })**: Decrements (negative increment).
- **init(...)**: Initializes model (called internally).
- **belongsTo(...)**, **hasOne(...)**, **hasMany(...)**, **belongsToMany(...)**: Define associations.

Private:

- resolveDefault: Handles NOW/UUIDV4.
- buildSelectQuery: Builds SQL for selects.
- nestAssociations: Nests included data.

## Query Options

`FindOptions`:

- `attributes?: string[]` (default: all)
- `where?: WhereOptions` (supports operators, and/or)
- `include?: IncludeOptions[]` (model, as, where, required, attributes)
- `order?: [string, 'ASC' | 'DESC'][]`
- `group?: string[]`
- `limit?: number`
- `offset?: number`
- `raw?: boolean` (flat results)

`WhereOptions`: Key-value or { [Op]: value }, arrays for IN, nested and/or.

## CRUD Operations

See examples in previous sections. All methods throw in freeTierMode for DML.

## Query Interface

Methods from `queryInterface.ts`:

- createTable(tableName, attributes, options: { partitionBy?, clusterBy? })
- dropTable(tableName)
- addColumn(tableName, columnName, type)
- removeColumn(tableName, columnName)
- renameColumn(tableName, oldName, newName)
- changeColumn(tableName, columnName, type) // Limited support
- addPartition(tableName, partitionBy) // Warns, requires recreation
- addClustering(tableName, clusterBy)
- query(sql, params?)

Accessed via `orm.getQueryInterface()`.

## Migrations

Migrations use `runMigrations(path)` and `revertLastMigration(path)`.

### Migration Files Structure

Files in `/migrations` as `.ts` or `.js`, exported as object with `up` and `down` async functions.

Example `/migrations/20230812-create-users.ts`:

```typescript
export default {
  async up(qi, orm) {
    await qi.createTable(
      "users",
      {
        id: DataTypes.INTEGER({ primaryKey: true, allowNull: false }),
        name: DataTypes.STRING({ allowNull: false }),
        email: DataTypes.STRING(),
        age: DataTypes.INTEGER(),
        createdAt: DataTypes.DATETIME({ defaultValue: DataTypes.NOW }),
      },
      { partitionBy: "createdAt", clusterBy: ["name"] }
    );
  },
  async down(qi, orm) {
    await qi.dropTable("users");
  },
};
```

- `qi`: QueryInterface
- `orm`: BigQueryORM instance
- Files sorted alphabetically, executed if not recorded.

In freeTierMode, tracked in-memory.

### Running Migrations

```javascript
await orm.runMigrations("./migrations");
await orm.revertLastMigration("./migrations"); // Reverts last executed
```

## Free Tier Mode

- Disables DML (INSERT/UPDATE/DELETE/ALTER).
- Limits table creation/deletion to storage quotas.
- Migrations tracked in-memory.

## Operators (Op)

From `op.ts`: eq, ne, gt, gte, lt, lte, like, notLike, in, notIn, between, notBetween, is, isNot, and, or, not, any, all, contains, contained, add.

Used in `where`: { age: { [Op.gte]: 18 } }

## Syncing Schema

`orm.sync({ force?: boolean, alter?: boolean })`

- Creates dataset/tables if missing.
- force: Drops and recreates.
- alter: Warns (not fully supported).

## Transactions

`orm.transaction(async (qi) => { ... })`

- Limited in free tier to SELECT.

## Loading Models

`orm.loadModels(path)`: Loads `.ts`/`.js` files, calls default export with (orm, DataTypes), then associates.

## Error Handling

- Authentication failures.
- Free tier restrictions.
- Streaming buffer errors (wait and retry).
- Missing required fields.
- Association not found.

## Logging

`logging: true` logs creations, executions, etc.

## Contributing

Fork, PR to [repo].

## License

MIT
