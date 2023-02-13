// Copyright 2022 IBM Corp.
// SPDX-License-Identifier: Apache-2.0

package databasetypes

import (
	zerolog "github.com/rs/zerolog"

	models "fybrik.io/datacatalog-connector/datacatalog-go-models"
)

type DatabaseType interface {
	// DataCatalogTypeName() returns the Data Catalog name for different connections types. For instance, it
	// return "Mysql" for MYSQL and "Deltalake" for s3
	DataCatalogTypeName() string

	// translate the connection information from the Fybrik format to the Data Catalog format.
	// 'credentials' is the URI of a vault secret
	TranslateFybrikConfigToDataCatalogConfig(config map[string]interface{},
		connectionType string, credentials *string) map[string]interface{}

	// translate the connection information from the Data Catalog format to the Fybrik format.
	// also returns the connection type
	TranslateDataCatalogConfigToFybrikConfig(tableName string,
		config map[string]interface{}) (map[string]interface{}, string, error)

	// In checking whether a certain databaseService already exists, compare whether two
	// Data Catalog configuration informations are equivalent.Return 'true' if they are
	EquivalentServiceConfigurations(map[string]interface{}, map[string]interface{}) bool

	// TableName returns the name of the asset Table, e.g. '"fake.csv"'
	TableName(createAssetRequest *models.CreateAssetRequest) (string, error)
}

type dataBase struct {
	name   string
	logger *zerolog.Logger
}

func (db dataBase) DataCatalogTypeName() string {
	return db.name
}

// TableFQN returns the Fully Qualified Name of the asset DatabaseSchema,
// e.g. 'datacatalog-s3.default.fake-csv-bucket."fake.csv"'
func TableFQN(p DatabaseType, serviceName string, createAssetRequest *models.CreateAssetRequest) (string, error) {
	return "AssetID", nil
}
