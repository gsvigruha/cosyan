# Cosyan DB

Cosyan DB is an SQL based transactional RDBMS with sophisticated multi table constraint logic. The long term goal of
the project is to eliminate the need to manually program the business logic layer (like Java EE). This approach comes with immediate benefits:

 * Faster development cycle: no need for coding in Java EE, wait for releases, new deployment,
   just submit the logic as an SQL statement.
 * Dependency tracking: no need to think about which constraint can be broken by a particular `insert`,
   `update` or `delete` on a certain table. This is something triggers usually don not cover.
 * Performance: no need for multiple queries to check constraints. More, this integrated can optimize constraint evaluation
   by caching.

Users, contributors and constructive feedback is welcome! Please don't forget to *hit a star* if you like the project.

## Setup

Cosyan DB can be used as a standalone database server, or it can be embedded in a JVM app.

### Standalone database server

 * Install Java 8 or later.
 * Download a release from [here](http://cosyandb.com/releases/), or clone this repo and compile from source with Gradle.
 * Unpack the JAR and web files into a `destDir`.
 * [Configure](http://cosyandb.com/configuration/) cosyan.db.properties under the `destDir/conf` dir.
 * Start and stop the database with `start.sh` and `stop.sh`.
 * Use the built in Web UI or use the HTTP API to submit queries or edit the data directly.
 
### Embed in your JVM project

Alternatively you can embed it into a Java project and submit queries through the [Java API](https://github.com/gsvigruha/cosyan/blob/master/src/main/java/com/cosyan/db/DBApi.java).
```
Config config = new Config("destDir/conf");
DBApi dbApi = new DBApi(config);
Session session = dbApi.newAdminSession();
JSONObject result = session.execute("select * from table;").toJSON();
```

## Usage

In addition to SQL, extra features are introduced to help with multi-table constraints:

 * Columns in other tables can be directly referred to via chains of [foreign keys](https://github.com/gsvigruha/cosyan/blob/master/src/main/resources/doc/rules/31_foreign_keys.md).
 * Aggregated views can be defined on one to many relationships - i.e. [reverse foreign keys](https://github.com/gsvigruha/cosyan/blob/master/src/main/resources/doc/rules/32_reverse_foreign_keys.md) - and used in constraints.

Cosyan DB supports the following SQL DML features:

 * Select
 * Distinct
 * Where
 * Group by
 * Having
 * Order by
 * Inner, left and right join
 * Arithmetic and logical expressions
 * Insert
 * Delete
 * Update

DDL features:

 * Create/drop table
 * Create/drop index (on one attribute)
 * Alter table add/drop column
 * Alter table add/drop constraint
