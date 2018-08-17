# Cosyan DB

Cosyan DB is a transactional RDBMS with sophisticated multi table constraint logic. The long term goal of the project is
to eliminate the need to manually program the business logic layer (like Java EE). This approach comes with immediate benefits:

 * Much faster development cycle
 * Dependency tracking for free
 * Performance

## Setup

### Standalone app

 * Install Java 8 or later.
 * Clone this repo and compile from source with Gradle, or download a release from [here](http://cosyandb.com/releases/).
 * [Configure](http://cosyandb.com/configuration/) cosyan.db.properties under the `conf` dir.
 * Start and stop the database with `start.sh` and `stop.sh`.
 * Use the built in Web UI or use the HTTP API to submit queries.
 
### Embed in your project

 * Alternatively you can embed it into a Java project and submit queries through the Java API.
