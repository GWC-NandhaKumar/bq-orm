# BigQuery ORM (orm-bq)

[![npm version](https://badge.fury.io/js/orm-bq.svg)](https://badge.fury.io/js/orm-bq) [![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) [![Build Status](https://travis-ci.org/your-org/orm-bq.svg?branch=main)](https://travis-ci.org/your-org/orm-bq)

**orm-bq** is a lightweight Object-Relational Mapping (ORM) library for Google BigQuery, inspired by Sequelize but tailored for BigQuery's serverless, columnar database architecture. It simplifies interactions with BigQuery by providing model definitions, associations, migrations, and query interfaces while supporting multi-dataset operations (no global dataset config—pass dataset names dynamically to methods).

This ORM handles schema management, CRUD operations, associations (hasOne, hasMany, belongsTo, belongsToMany), and more. It includes free-tier mode for cost-aware development and logging for debugging.

**Key Highlights:**

- Multi-dataset support: Pass dataset as an argument to methods for flexibility across projects.
- BigQuery-specific optimizations: Uses clustering on primary keys for query performance (BigQuery's equivalent to indexing).
- Association support with nested queries.
- Migration system with up/down methods.
- Free-tier mode to restrict DML operations and warn on potential costs.
- TypeScript support with strong typing for models and queries.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Defining Models](#defining-models)
- [Loading Models](#loading-models)
- [Syncing Schema](#syncing-schema)
- [CRUD Operations](#crud-operations)
- [Associations](#associations)
- [Queries and Aggregations](#queries-and-aggregations)
- [Migrations](#migrations)
- [Query Interface](#query-interface)
- [Transactions](#transactions)
- [Free-Tier Mode](#free-tier-mode)
- [Logging](#logging)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Model Definition**: Define schemas with data types (STRING, INT64, TIMESTAMP, ARRAY, STRUCT, etc.).
- **Associations**: belongsTo, hasOne, hasMany, belongsToMany with automatic joins.
- **CRUD**: create, bulkCreate, findAll, findOne, update, destroy, etc.
- **Queries**: where clauses with operators (eq, gt, in, etc.), includes, order, limit, offset.
- **Aggregations**: count, max, min, sum, average.
- **Migrations**: Run and revert schema changes.
- **Query Interface**: Direct DDL/DML like createTable, addColumn, etc.
- **Multi-Dataset**: No fixed dataset in config; pass as method arg for isolation.
- **Optimizations**: Automatic clustering on primary keys during table creation.
- **Safety**: Free-tier mode blocks DML to avoid costs.
- **Logging**: Configurable logging for operations.

## Installation

Install via npm:

```bash
npm install orm-bq
```

## Configuration

Create an instance of `BigQueryORM` with your project config.  
No dataset is required in config — pass it dynamically when calling methods.

```typescript
import { BigQueryORM } from "orm-bq";

// Example with local key file
const orm = new BigQueryORM({
  projectId: "your-project-id",
  keyFilename: "/path/to/keyfile.json", // Optional if using env
  logging: true, // Enable logging (default: false)
  freeTierMode: false, // Enable for cost restrictions (default: false)
});

await orm.authenticate(); // Verify connection
```

### 🔑 Authentication Options

`BigQueryORM` supports three authentication modes:

1. **Application Default Credentials (ADC)**

   - Works automatically on GCP (Cloud Run, GCE, GKE, App Engine).
   - Also works locally if you run:

     ```bash
     gcloud auth application-default login
     ```

   - No extra config required.

2. **Environment variable with inline JSON (recommended for non-GCP hosts like Render, Heroku, AWS):**

   - Store your service account JSON in an env var:

     ```
     GOOGLE_APPLICATION_CREDENTIALS_JSON = { ...full JSON key... }
     ```

   - `BigQueryORM` will automatically detect and use it.

3. **Key file path:**

   - Provide the service account file path via `keyFilename` config:

     ```typescript
     const orm = new BigQueryORM({
       projectId: "your-project-id",
       keyFilename: "/path/to/keyfile.json",
     });
     ```

## Defining Models

Models extend `Model` and define attributes using `DataTypes`.

```typescript
import { DataTypes } from "orm-bq";

const User = orm.define(
  "User",
  {
    id: {
      type: DataTypes.INTEGER(),
      primaryKey: true,
      allowNull: false,
    },
    name: DataTypes.STRING({ allowNull: false }),
    email: DataTypes.STRING(),
    createdAt: {
      type: DataTypes.DATE(),
      defaultValue: DataTypes.NOW,
    },
  },
  {
    tableName: "users", // Optional: defaults to lowercase model name
    primaryKey: "id", // Optional: defaults to 'id'
  }
);
```

## Loading Models

Load multiple models from a directory (e.g., `./models`). Each file exports a function that defines the model.

```typescript
// models/user.ts
export default (orm, DataTypes) => {
  const User = orm.define("User", {
    // attributes...
  });
  return User;
};

// Load all
await orm.loadModels("./models");
```

Associations can be defined in an `associate` static method:

```typescript
// In user.ts
User.associate = (models) => {
  User.hasMany(models.Post, { as: "posts" });
};
```

## Syncing Schema

Sync models to BigQuery tables. Creates datasets/tables if missing, with clustering on primary key.

```typescript
await orm.sync("my_dataset", { force: true }); // force: Drop and recreate tables
```

- **Options**:
  - `force`: boolean (default: false) - Drop existing tables.
  - `alter`: boolean (default: false) - Alter tables (limited support).

## CRUD Operations

All methods require a `dataset` argument.

### Create

```typescript
const user = await User.create("my_dataset", {
  name: "John Doe",
  email: "john@example.com",
});
```

### Bulk Create

```typescript
await User.bulkCreate(
  "my_dataset",
  [
    { name: "Jane", email: "jane@example.com" },
    { name: "Bob", email: "bob@example.com" },
  ],
  { validate: true }
);
```

### Find

```typescript
const users = await User.findAll("my_dataset", {
  where: { name: { [Op.like]: "%John%" } },
  limit: 10,
  order: [["createdAt", "DESC"]],
});

const singleUser = await User.findOne("my_dataset", { where: { id: 1 } });

const userByPk = await User.findByPk("my_dataset", 1);
```

### Update

```typescript
const updatedCount = await User.update(
  "my_dataset",
  { name: "New Name" },
  {
    where: { id: 1 },
  }
);
```

### Destroy

```typescript
const deletedCount = await User.destroy("my_dataset", { where: { id: 1 } });
```

### Truncate

```typescript
await User.truncate("my_dataset");
```

## Associations

Define relationships:

```typescript
User.hasMany(Post, { foreignKey: "userId", as: "posts" });
Post.belongsTo(User, { foreignKey: "userId", as: "user" });
```

Query with includes:

```typescript
const usersWithPosts = await User.findAll("my_dataset", {
  include: [{ model: Post, as: "posts", required: true }],
});
```

For belongsToMany:

```typescript
User.belongsToMany(Group, { through: GroupUser });
```

## Queries and Aggregations

### Find and Count

```typescript
const { rows, count } = await User.findAndCountAll('my_dataset', {
  where: { ... },
  limit: 10,
  offset: 20,
});
```

### Aggregations

```typescript
const totalUsers = await User.count("my_dataset");
const maxAge = await User.max("my_dataset", "age");
const minAge = await User.min("my_dataset", "age");
const sumSalary = await User.sum("my_dataset", "salary");
const avgSalary = await User.average("my_dataset", "salary");
```

### Increment/Decrement

```typescript
await User.increment('my_dataset', 'age', { by: 1, where: { id: 1 } });
await User.decrement('my_dataset', ['views', 'likes'], { by: 5, where: { ... } });
```

## Migrations

Migrations are JS/TS files in a directory (e.g., `./migrations`).

```typescript
// migrations/20230101-create-users.ts
export default {
  async up(queryInterface, orm, dataset) {
    await queryInterface.createTable(
      dataset,
      "users",
      {
        id: { type: orm.DataTypes.INTEGER, primaryKey: true },
        name: orm.DataTypes.STRING,
      },
      { primaryKey: "id" }
    ); // Clusters on 'id'
  },
  async down(queryInterface, orm, dataset) {
    await queryInterface.dropTable(dataset, "users");
  },
};
```

Run:

```typescript
await orm.runMigrations("my_dataset", "./migrations");
await orm.revertLastMigration("my_dataset", "./migrations");
```

## Query Interface

Access via `orm.getQueryInterface()`.

### Create/Drop Table

```typescript
const qi = orm.getQueryInterface();
await qi.createTable(
  "my_dataset",
  "new_table",
  {
    id: DataTypes.INTEGER({ primaryKey: true }),
  },
  { partitionBy: "createdAt", clusterBy: ["id"] }
);
await qi.dropTable("my_dataset", "new_table");
```

### Column Operations

```typescript
await qi.addColumn("my_dataset", "users", "newField", DataTypes.STRING());
await qi.removeColumn("my_dataset", "users", "oldField");
await qi.renameColumn("my_dataset", "users", "oldName", "newName");
await qi.changeColumn("my_dataset", "users", "field", DataTypes.INTEGER());
```

### Clustering/Partitioning

```typescript
await qi.addClustering("my_dataset", "users", ["id", "name"]);
await qi.addPartition("my_dataset", "users", "dateField"); // Requires recreation
```

### Raw Query

```typescript
const [rows] = await qi.query(
  "my_dataset",
  "SELECT * FROM `my_dataset.users` LIMIT 10"
);
```

## Transactions

Wrap operations (limited to SELECT in free-tier):

```typescript
await orm.transaction('my_dataset', async (qi, dataset) => {
  await qi.createTable(dataset, 'temp', { ... });
  // Other operations
});
```

## Free-Tier Mode

Enable in config to restrict DML (INSERT/UPDATE/DELETE) and warn on storage costs.

```typescript
new BigQueryORM({ freeTierMode: true });
```

- Blocks creates/updates/deletes.
- Uses in-memory migration tracking.
- Limits transactions to SELECT.

## Logging

Enabled via config or `BIGQUERY_ORM_LOGGING=true` env. Logs operations with context.

## Examples

### Full Setup Example

```typescript
import { BigQueryORM, DataTypes } from "orm-bq";

const orm = new BigQueryORM({ projectId: "project-id", logging: true });
await orm.loadModels("./models");
await orm.sync("test_dataset", { force: true });
await orm.runMigrations("test_dataset", "./migrations");

// CRUD
await User.create("test_dataset", { name: "Test User" });
const users = await User.findAll("test_dataset", {
  where: { name: { [Op.eq]: "Test User" } },
  include: [{ model: Post }],
});
```

### Association Example

```typescript
// Define models
const Post = orm.define("Post", { title: DataTypes.STRING });
User.hasMany(Post);

// Query
const userPosts = await User.findOne("dataset", {
  where: { id: 1 },
  include: [{ model: Post, as: "posts" }],
});
console.log(userPosts.posts); // Nested array
```

### Migration Example

See migration file above. Run with `orm.runMigrations`.

### Query with Operators

```typescript
import { Op } from "orm-bq";

await User.findAll("dataset", {
  where: {
    age: { [Op.gte]: 18, [Op.lte]: 65 },
    name: { [Op.like]: "%Doe%" },
    [Op.or]: [{ status: "active" }, { status: "pending" }],
  },
});
```

### Advanced Data Types

```typescript
orm.define("ComplexModel", {
  arrayField: DataTypes.ARRAY(DataTypes.STRING()),
  structField: DataTypes.STRUCT({
    sub1: DataTypes.INTEGER(),
    sub2: DataTypes.STRING(),
  }),
  geo: DataTypes.GEOGRAPHY(),
});
```

## Contributing

1. Fork the repo.
2. Create a branch: `git checkout -b feature/new-feature`.
3. Commit changes: `git commit -am 'Add new feature'`.
4. Push: `git push origin feature/new-feature`.
5. Submit a Pull Request.

Run tests: `npm test`.

## License

MIT License. See [LICENSE](LICENSE) for details.

---

This README is comprehensive and structured for easy navigation. For issues, open a GitHub issue. Contributions welcome!
