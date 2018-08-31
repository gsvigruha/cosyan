### HTTP API

#### Path `/cosyan/admin`<br/>
HTTP `GET` Params<br/>
Returns the metadata for all tables.<br/>
 * `token`: User authentication token.<br/>
<br/>

#### Path `/cosyan/login`<br/>
HTTP `GET` Params<br/>
Logs in to the DB and returns the user authentication token.<br/>
 * `username`, mandatory: The name of the DB user.<br/>
 * `password`, mandatory: The unencrypted password of the DB user.<br/>
 * `method`, mandatory: Authentication method (LOCAL, LDAP).<br/>
<br/>

#### Path `/cosyan/monitoring`<br/>
HTTP `GET` Params<br/>
Returns system monitoring info.<br/>
 * `token`: User authentication token.<br/>
<br/>

#### Path `/cosyan/users`<br/>
HTTP `GET` Params<br/>
Returns the list of uses and their grants.<br/>
 * `token`: User authentication token.<br/>
<br/>

#### Path `/cosyan/index`<br/>
HTTP `GET` Params<br/>
Looks up a key in an index and returns the corresponding values.<br/>
 * `token`: User authentication token.<br/>
 * `session`: Session ID.<br/>
 * `index`, mandatory: Full index name.<br/>
 * `key`, mandatory: The value in the index to check.<br/>
<br/>

#### Path `/cosyan/sql`<br/>
HTTP `GET` Params<br/>
Executes an SQL script and returns the results.<br/>
 * `token`: User authentication token.<br/>
 * `session`: Session ID.<br/>
 * `sql`, mandatory: The SQL script to execute.<br/>
<br/>

#### Path `/cosyan/cancel`<br/>
HTTP `GET` Params<br/>
Cancels the currently running query in the session.<br/>
 * `token`: User authentication token.<br/>
 * `session`, mandatory: Session ID.<br/>
<br/>

#### Path `/cosyan/createSession`<br/>
HTTP `GET` Params<br/>
Creates a session for a given user and returns the session ID.<br/>
 * `token`: User authentication token.<br/>
<br/>

#### Path `/cosyan/closeSession`<br/>
HTTP `GET` Params<br/>
Closes the session.<br/>
 * `token`: User authentication token.<br/>
 * `session`, mandatory: Session ID.<br/>
<br/>

#### Path `/cosyan/entityMeta`<br/>
HTTP `GET` Params<br/>
User authentication token.<br/>
 * `token`: User authentication token.<br/>
 * `session`: Session ID.<br/>
<br/>

#### Path `/cosyan/loadEntity`<br/>
HTTP `GET` Params<br/>
Returns the metadata for all entities.<br/>
 * `token`: User authentication token.<br/>
 * `session`: Session ID.<br/>
 * `table`, mandatory: Full table name.<br/>
 * `id`, mandatory: Value of the ID column of the table.<br/>
<br/>

