### Configuration

 * `DATA_DIR`<br/>
   `FILE, mandatory`: The directory containing the config files.

 * `LDAP_HOST`<br/>
   `STRING`: The hostame of the LDAP server.

 * `LDAP_PORT`<br/>
   `INT`: The port of the LDAP server.

 * `AUTH`<br/>
   `BOOL, mandatory`: Whether authentication is enabled or not.

 * `PORT`<br/>
   `INT, mandatory`: The port Cosyan server listens on.

 * `WEBSERVER_NUM_THREADS`<br/>
   `INT, mandatory`: The number of threads for the webserver.

 * `DB_NUM_THREADS`<br/>
   `INT, mandatory`: The number of threads for the DB.

 * `TR_RETRY_MS`<br/>
   `INT, mandatory`: The amount of time tasks sleep before trying to acquire locks again.

