// Copyright 2022 IBM Corp.
// SPDX-License-Identifier: Apache-2.0

package databasetypes

import (
	"reflect"

	"github.com/rs/zerolog"

	models "fybrik.io/datacatalog-connector/datacatalog-go-models"
)

type generic struct {
	dataBase
}

func NewGeneric(logger *zerolog.Logger) *generic {
	return &generic{dataBase: dataBase{name: CustomDatabase, logger: logger}}
}

// Data Catalog databaseService-s of type CustomDatabase expect the configuration key-value pairs to be placed
// within the 'connectionOptions' field. The values in the key-value pairs must be strings
func (m *generic) TranslateFybrikConfigToDataCatalogConfig(config map[string]interface{},
	connectionType string, credentials *string) map[string]interface{} {
	return config
}

// take the configuration key-value pairs from the `connectionOptions` field, and convert the
// JSON fields to maps
func (m *generic) TranslateDataCatalogConfigToFybrikConfig(tableName string,
	config map[string]interface{}) (map[string]interface{}, string, error) {
	return config, "connectionType", nil
}

func (m *generic) EquivalentServiceConfigurations(requestConfig, serviceConfig map[string]interface{}) bool {
	return reflect.DeepEqual(requestConfig, serviceConfig)
}

func (m *generic) DatabaseName(createAssetRequest *models.CreateAssetRequest) string {
	return Default
}

func (m *generic) DatabaseSchemaName(createAssetRequest *models.CreateAssetRequest) string {
	return createAssetRequest.DestinationCatalogID
}

func (m *generic) TableName(createAssetRequest *models.CreateAssetRequest) (string, error) {
	return *createAssetRequest.DestinationAssetID, nil
}
