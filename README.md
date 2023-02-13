# datacatalog-connector
Generic Fybrik Connector.
Implements the Fybrik Data-Catalog specification.

## Compiling and Running
The first time you wish to compile the Connector, run:
```bash
make
```
`make` automatically generates prerequisite code and compile the connector.

The next times you want to compile, run:
```bash
make compile
```

To run the connector locally, run:
```bash
make run
```

`make run` runs the connector with the configuration file in `conf/conf.yaml`. This configuration assumes that the data catalog is running on localhost and listening on port 8585. In addition, it assumes that Vault is running on localhost too and listening on port 8200. If that is not the case, change the configuration file or employ port-forwarding.
The configuration file also contains a path to a JWT file which is used to identify against Vault.

### Experiment with Connector
1.
```bash
curl -X POST localhost:8081/createAsset -d @mysql.json
```
2.
```bash
curl -X POST localhost:8081/getAssetInfo -d '{"assetID": "AssetID", "operationType": "read"}'
```

## Directory Structure
- [pkg/database-types](pkg/database-types): Currently, could support data sources such as mysql and s3. The direcory contains the
[database_type.go](pkg/database-types/database_type.go) file, which defines the DatabaseType interface.
- [pkg/datacatalog-connector-core](pkg/datacatalog-connector-core): The core files of datacatalog connector, they 
are implement the connector REST API.  
- [pkg/utils](pkg/utils): Includes utility methods used in the connector code
- [pkg/vault](pkg/vault): Includes methods to obtain a token and secrets from Vault
- [conf](conf): Contains a sample configuration file
- [auto-generated](auto-generated): Automatically generated OpenAPI code, both for data catalog (client code) and Fybrik Data Catalog (server code)
