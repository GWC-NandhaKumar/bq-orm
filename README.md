# BigQuery ORM (`bq-orm`)

`bq-orm` is a Sequelize-inspired Object-Relational Mapping (ORM) library for Google BigQuery, designed for Node.js developers. It provides familiar ORM patterns for model definitions, associations (relationships), CRUD operations, migrations, advanced querying (with search, sort, pagination), and support for BigQuery's unique features like partitioning, clustering, structured data types (`STRUCT`, `ARRAY`, `JSON`), geography, intervals, and free-tier limitations.

This README is a comprehensive, developer-friendly guide, with detailed explanations, code snippets, and examples for all features. It's based on the library's source code as of August 25, 2025, and assumes TypeScript usage for type safety (though JavaScript works too). The library is TypeScript-first, with strong typing for models, migrations, queries, and more.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Authentication](#authentication)
- [Defining Models](#defining-models)
  - [Basic Model Definition](#basic-model-definition)
  - [Model Files and Loading](#model-files-and-loading)
- [Supported Data Types](#supported-data-types)
  - [Examples for Advanced Types](#examples-for-advanced-types)
- [Relationships (Associations)](#relationships-associations)
  - [One-to-One (belongsTo, hasOne)](#one-to-one-belongsto-hasone)
  - [One-to-Many (hasMany)](#one-to-many-hasmany)
  - [Many-to-Many (belongsToMany)](#many-to-many-belongstomany)
- [Model Methods](#model-methods)
  - [CRUD Methods (Create, Update, Destroy)](#crud-methods-create-update-destroy)
  - [Query Methods (findAll, findOne, findByPk, count)](#query-methods-findall-findone-findbypk-count)
  - [Utility Methods (Increment, Decrement)](#utility-methods-increment-decrement)
- [Querying Data](#querying-data)
  - [Where Clauses and Operators](#where-clauses-and-operators)
  - [Searching (LIKE, IN, etc.)](#searching-like-in-etc)
  - [Sorting (ORDER BY)](#sorting-order-by)
  - [Pagination (LIMIT, OFFSET)](#pagination-limit-offset)
  - [Grouping and Aggregations](#grouping-and-aggregations)
  - [Eager Loading (Include)](#eager-loading-include)
  - [Raw Queries](#raw-queries)
- [CRUD Operations with Examples](#crud-operations-with-examples)
- [Query Interface](#query-interface)
  - [Schema Operations](#schema-operations)
- [Creating Datasets and Tables](#creating-datasets-and-tables)
- [Migrations](#migrations)
  - [Migration Files Structure](#migration-files-structure)
  - [Running and Reverting Migrations](#running-and-reverting-migrations)
  - [Migration Examples](#migration-examples)
- [Schema Syncing](#schema-syncing)
- [Transactions](#transactions)
- [Free Tier Mode](#free-tier-mode)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [Best Practices and Tips](#best-practices-and-tips)
- [Contributing](#contributing)
- [License](#license)

## Features

- **TypeScript-First**: Strong typing for models, attributes, queries, migrations, and associations.
- **Model Definitions**: Attributes with defaults, nullability, primary keys, and auto-generated fields (e.g., UUID, timestamps).
- **Data Types**: Full BigQuery support, including `ARRAY`, `STRUCT`, `JSON`, `GEOGRAPHY`, `INTERVAL`, and more.
- **Associations**: 1:1, 1:n, n:m relationships with automatic JOINs and eager loading.
- **CRUD Operations**: Create, read (with search/sort/pagination), update, delete, bulk operations.
- **Query Building**: Advanced `WHERE` with operators, `INCLUDE` for joins, `ORDER`, `GROUP`, `LIMIT/OFFSET`.
- **Migrations**: Up/down scripts for schema changes, with in-memory tracking for free tier.
- **Schema Tools**: Create/drop datasets/tables, add/remove/rename columns, partitioning/clustering.
- **Free Tier Support**: Restricts DML to avoid billing; limits queries to 1TB/month.
- **Logging**: Configurable console logging for operations, errors, and queries.
- **Transactions**: Basic support (limited in free tier).
- **Error Handling**: Descriptive errors for authentication, missing fields, free-tier violations.

## Installation

```bash
npm install bq-orm @google-cloud/bigquery
```

- `bq-orm`: The ORM library.
- `@google-cloud/bigquery`: Peer dependency for BigQuery client.

For TypeScript, no additional setup is needed—the library includes type definitions.

## Configuration

Create an instance of `BigQueryORM`:

```typescript
import { BigQueryORM } from "bq-orm";

const orm = new BigQueryORM({
  projectId: "your-project-id", // Required
  dataset: "your-dataset", // Required; prompts if unset in CLI
  keyFilename: "/path/to/key.json", // Optional for auth
  logging: true, // Enable console logs (default: false)
  freeTierMode: false, // Enable for free-tier restrictions (default: false)
});
```

Environment variables (preferred for production):

- `GOOGLE_CLOUD_PROJECT`: Project ID.
- `BIGQUERY_DATASET`: Dataset name.
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account key.
- `BIGQUERY_ORM_LOGGING`: "true" to enable logging.

## Authentication

Verify credentials:

```typescript
try {
  await orm.authenticate();
  console.log("Authenticated successfully");
} catch (err) {
  console.error("Authentication failed:", err);
}
```

- Throws if invalid credentials or network issues.
- In free-tier mode, warns about SELECT-only limitations.

## Defining Models

Models extend an abstract `Model` class and are defined with attributes.

### Basic Model Definition

```typescript
import { BigQueryORM, DataTypes } from "bq-orm";

const User = orm.define(
  "User",
  {
    id: DataTypes.INTEGER({ primaryKey: true, allowNull: false }),
    name: DataTypes.STRING({ allowNull: false }),
    email: DataTypes.STRING({ allowNull: false }),
    age: DataTypes.INTEGER(),
    createdAt: DataTypes.DATETIME({ defaultValue: DataTypes.NOW }),
    uuid: DataTypes.UUID({ defaultValue: DataTypes.UUIDV4 }),
  },
  {
    tableName: "users", // Optional: defaults to lowercase model name
    primaryKey: "id", // Optional: defaults to "id" or first primaryKey attribute
  }
);
```

- `attributes`: Record of field names to `DataType`.
- Options: `tableName`, `primaryKey`.

### Model Files and Loading

For larger projects, define models in separate files under `/models`. Each file exports a function:

`/models/user.ts`:

```typescript
import { BigQueryORM, DataTypes as DataTypesType } from "bq-orm";

export default (orm: BigQueryORM, DataTypes: DataTypesType) => {
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
    { tableName: "users" }
  );

  User.associate = (models: Record<string, typeof Model>) => {
    orm.logger.info("[User:associate] Setting up associations");
    User.hasMany(models.Order, { foreignKey: "userId", as: "orders" });
  };

  return User;
};
```

Load all models:

```typescript
await orm.loadModels("./models"); // Loads .ts/.js files, defines models, runs associates
```

- Automatically calls `associate` if defined.
- Models are stored in `orm.models` (e.g., `orm.models.User`).

## Supported Data Types

All BigQuery types are supported via `DataTypes`:

- Basic: `STRING()`, `INTEGER()`, `FLOAT()`, `BOOLEAN()`, `DATE()`, `TIME()`, `DATETIME()`.
- Aliases: `CHAR()`, `TEXT()` → `STRING`; `DOUBLE()` → `FLOAT`; `TINYINT()` etc. → `INTEGER`.
- Precision: `DECIMAL(precision, scale)` e.g., `DECIMAL(10, 2)`.
- Advanced: `JSON()`, `BYTES()`, `UUID()` (STRING with UUIDV4 default).
- Collections: `ARRAY(itemType)` e.g., `ARRAY(DataTypes.STRING())`.
- Nested: `STRUCT(fields)` e.g., `STRUCT({ name: DataTypes.STRING() })`.
- Spatial/Temporal: `GEOGRAPHY()`, `INTERVAL()`.
- Defaults: `NOW` (`CURRENT_TIMESTAMP()`), `UUIDV4` (`GENERATE_UUID()`).

Options for types: `{ allowNull: boolean, defaultValue: any, primaryKey: boolean, mode: 'REPEATED', fields: Record<string, DataType>, precision: number, scale: number }`.

### Examples for Advanced Types

- JSON:

  ```typescript
  preferences: DataTypes.JSON(), // Store { key: value }
  ```

  Query example (raw): `SELECT JSON_VALUE(preferences, '$.key') FROM users`.

- ARRAY:

  ```typescript
  tags: DataTypes.ARRAY(DataTypes.STRING()), // Store ["tag1", "tag2"]
  ```

  Query example: `SELECT * FROM posts WHERE 'tag1' IN UNNEST(tags)`.

- STRUCT:

  ```typescript
  address: DataTypes.STRUCT({ street: DataTypes.STRING(), city: DataTypes.STRING() }), // Store { street: "123 Main", city: "NY" }
  ```

  Query example: `SELECT address.city FROM users`.

## Relationships (Associations)

Associations define relationships between models. Define them in `associate` functions. The library handles JOINs automatically in queries with `include`.

### One-to-One (belongsTo, hasOne)

- `belongsTo`: Target owns the foreign key (e.g., Order belongs to User).

  `/models/order.ts`:

  ```typescript
  Order.belongsTo(User, { foreignKey: "userId", as: "user" }); // Adds userId to Order
  ```

- `hasOne`: Source owns the relationship (e.g., User has one Profile).

  `/models/user.ts`:

  ```typescript
  User.hasOne(Profile, { foreignKey: "userId", as: "profile" }); // Profile has userId
  ```

Example Query:

```typescript
const order = await Order.findOne({
  include: [{ model: User, as: "user" }],
});
console.log(order.user.name); // Nested user data
```

### One-to-Many (hasMany)

- Source has multiple targets (e.g., User has many Orders).

  `/models/user.ts`:

  ```typescript
  User.hasMany(Order, { foreignKey: "userId", as: "orders" });
  ```

  `/models/order.ts`:

  ```typescript
  Order.belongsTo(User, { foreignKey: "userId", as: "user" });
  ```

Example Query:

```typescript
const user = await User.findOne({
  include: [
    { model: Order, as: "orders", where: { amount: { [Op.gt]: 100 } } },
  ],
});
console.log(user.orders.length); // Array of orders
```

### Many-to-Many (belongsToMany)

- Uses a junction model (through).

  `/models/post.ts`:

  ```typescript
  Post.belongsToMany(Tag, {
    through: PostTag,
    foreignKey: "postId",
    otherKey: "tagId",
    as: "tags",
  });
  ```

  `/models/tag.ts`:

  ```typescript
  Tag.belongsToMany(Post, {
    through: PostTag,
    foreignKey: "tagId",
    otherKey: "postId",
    as: "posts",
  });
  ```

  Junction model `/models/postTag.ts`:

  ```typescript
  const PostTag = orm.define(
    "PostTag",
    {
      postId: DataTypes.INTEGER({ allowNull: false }),
      tagId: DataTypes.INTEGER({ allowNull: false }),
    },
    { tableName: "post_tags" }
  );
  ```

Example Query:

```typescript
const post = await Post.findOne({
  include: [{ model: Tag, as: "tags" }],
});
console.log(post.tags.map((t) => t.name)); // Array of tags
```

## Model Methods

All methods are static on the model class (e.g., `User.findAll()`).

### CRUD Methods (Create, Update, Destroy)

- `create(data)`: Insert one record.
- `bulkCreate(dataArray)`: Insert multiple.
- `update(data, { where })`: Update matching records, returns affected count.
- `destroy({ where })`: Delete matching, returns affected count.

### Query Methods (findAll, findOne, findByPk, count)

- `findAll(options)`: Fetch all with options; returns array (nested for includes).
- `findOne(options)`: Fetch first or null.
- `findByPk(pk, options)`: Fetch by primary key.
- `count(options)`: Count distinct primary keys.

### Utility Methods (Increment, Decrement)

- `increment(fields, { by: number, where })`: Increment fields by value (default 1).
- `decrement(fields, { by: number, where })`: Decrement (negative increment).

## Querying Data

### Where Clauses and Operators

Use `Op` for conditions:

```typescript
import { Op } from "bq-orm";

await User.findAll({
  where: {
    age: { [Op.gte]: 18, [Op.lt]: 65 }, // AND
    [Op.or]: [{ name: "John" }, { email: "john@example.com" }], // OR
    tags: { [Op.contains]: "developer" }, // For ARRAY
  },
});
```

Operators (`Op`): eq, ne, gt, gte, lt, lte, like, notLike, in, notIn, between, notBetween, is, isNot, and, or, not, any, all, contains, contained, add.

### Searching (LIKE, IN, etc.)

- String search: `{ name: { [Op.like]: "%John%" } }`
- Array search: `{ id: { [Op.in]: [1, 2, 3] } }`
- Range: `{ age: { [Op.between]: [18, 30] } }`

Example:

```typescript
const users = await User.findAll({
  where: {
    name: { [Op.like]: "%Doe%" },
    tags: { [Op.contains]: "orm" }, // ARRAY contains "orm"
  },
});
```

### Sorting (ORDER BY)

```typescript
await User.findAll({
  order: [
    ["age", "DESC"],
    ["name", "ASC"],
  ],
});
```

### Pagination (LIMIT, OFFSET)

```typescript
await User.findAll({
  limit: 10,
  offset: 20, // Skip first 20
});
```

### Grouping and Aggregations

```typescript
await User.findAll({
  attributes: ["age", "COUNT(*) AS count"], // Use raw for aggregates
  group: ["age"],
  raw: true, // Flat results
});
```

For complex aggregates, use raw queries.

### Eager Loading (Include)

Load associations:

```typescript
await User.findAll({
  include: [
    {
      model: Order,
      as: "orders",
      required: true,
      attributes: ["amount"],
      where: { amount: { [Op.gt]: 50 } },
    },
  ],
});
```

- `required: true`: INNER JOIN (must have association).
- `attributes`: Select specific fields from included model.

### Raw Queries

Use `QueryInterface` for custom SQL:

```typescript
const qi = orm.getQueryInterface();
const [rows] = await qi.query("SELECT * FROM users WHERE age > @age", {
  age: 18,
});
```

## CRUD Operations with Examples

- Create:

  ```typescript
  const user = await User.create({
    name: "Jane Doe",
    email: "jane@example.com",
    age: 25,
  });
  ```

- Bulk Create:

  ```typescript
  await User.bulkCreate([
    { name: "Alice", email: "alice@example.com" },
    { name: "Bob", email: "bob@example.com" },
  ]);
  ```

- Update:

  ```typescript
  const updatedCount = await User.update({ age: 26 }, { where: { id: 1 } });
  ```

- Destroy:

  ```typescript
  const deletedCount = await User.destroy({ where: { age: { [Op.lt]: 18 } } });
  ```

- Increment:

  ```typescript
  await User.increment("age", { by: 1, where: { id: 1 } });
  ```

## Query Interface

### Schema Operations

```typescript
const qi = orm.getQueryInterface();
await qi.addColumn("users", "bio", DataTypes.TEXT());
await qi.removeColumn("users", "age");
await qi.renameColumn("users", "email", "emailAddress");
await qi.changeColumn("users", "age", DataTypes.INTEGER({ allowNull: true }));
await qi.addClustering("users", ["name"]);
```

- `addPartition`: Warns; requires manual recreation for existing tables.

## Creating Datasets and Tables

- Dataset:

  ```typescript
  await orm.createDataset({ location: "US", labels: { env: "dev" } });
  ```

- Table (via QueryInterface):

  ```typescript
  await qi.createTable(
    "profiles",
    {
      id: DataTypes.INTEGER({ primaryKey: true }),
      userId: DataTypes.INTEGER({ allowNull: false }),
      bio: DataTypes.TEXT(),
    },
    { partitionBy: "createdAt", clusterBy: ["userId"] }
  );
  ```

## Migrations

Migrations manage schema changes via scripts.

### Migration Files Structure

Files in `/migrations` (e.g., `20250825_create_profiles.ts`):

```typescript
import { QueryInterface } from "bq-orm";
import { BigQueryORM, DataTypes } from "bq-orm";

export default {
  async up(qi: QueryInterface, orm: BigQueryORM): Promise<void> {
    orm.logger.info("[Migration:create_profiles] Creating profiles table");
    await qi.createTable(
      "profiles",
      {
        id: DataTypes.INTEGER({ primaryKey: true, allowNull: false }),
        userId: DataTypes.INTEGER({ allowNull: false }),
        bio: DataTypes.TEXT(),
      },
      { clusterBy: ["userId"] }
    );
  },
  async down(qi: QueryInterface, orm: BigQueryORM): Promise<void> {
    await qi.dropTable("profiles");
  },
};
```

- Use `qi` for schema ops, `orm.logger` for logging.
- Files sorted alphabetically; executed if not already run.

### Running and Reverting Migrations

```typescript
await orm.runMigrations("./migrations"); // Applies pending migrations
await orm.revertLastMigration("./migrations"); // Reverts the last one
```

- In free-tier: In-memory tracking; no DML.

### Migration Examples

- Add Column Migration (`20250826_add_column.ts`):

  ```typescript
  export default {
    async up(qi: QueryInterface, orm: BigQueryORM): Promise<void> {
      await qi.addColumn("users", "phone", DataTypes.STRING());
    },
    async down(qi: QueryInterface, orm: BigQueryORM): Promise<void> {
      await qi.removeColumn("users", "phone");
    },
  };
  ```

- Relationship Migration: Create junction table for many-to-many.

## Schema Syncing

Automatically create/update tables based on models:

```typescript
await orm.sync({ force: true }); // Drop and recreate tables
await orm.sync({ alter: true }); // Alter (limited support)
```

- Use migrations for production.

## Transactions

```typescript
await orm.transaction(async (qi) => {
  await qi.query("INSERT INTO users (name) VALUES ('Test')");
});
```

- Limited to SELECT in free-tier.

## Free Tier Mode

Set `freeTierMode: true`:

- Disables INSERT/UPDATE/DELETE/ALTER.
- Warns on storage-impacting ops (e.g., table create).
- Migration tracking in-memory.

Enable billing at https://console.cloud.google.com/billing for full features.

## Logging

- Enabled via `logging: true` or env var.
- Logs: Operations, queries, errors (e.g., `[Model:findAll] Found 5 records`).
- Access: `orm.logger.info("Message", { data })`.

## Error Handling

- Authentication: Throws on failure.
- Missing Fields: Throws in create/update.
- Free Tier: Throws on restricted ops.
- Associations: Throws if invalid (e.g., missing through model).
- Streaming Buffer: Suggest retry for recent inserts.

Catch errors in try/catch blocks.

## Best Practices and Tips

- **Performance**: Use clustering/partitioning for large tables.
- **Queries**: Prefer parameterized queries to avoid SQL injection.
- **Relations**: Define bidirectional for full eager loading.
- **Migrations**: Version files with dates (YYYYMMDD_name.ts).
- **Testing**: Mock BigQuery client for unit tests.
- **Limitations**: No real-time transactions; BigQuery is analytical.
- **Debugging**: Enable logging; use raw queries for complex ops.

## Contributing

Fork the repo, add features/fixes, submit PRs. Follow code style from source.

## License

MIT
