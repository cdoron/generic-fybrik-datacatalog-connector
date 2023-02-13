// Copyright 2022 IBM Corp.
// SPDX-License-Identifier: Apache-2.0

package openapiconnectorcore

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/rs/zerolog"

	client "fybrik.io/datacatalog-connector/datacatalog-go-client"
	models "fybrik.io/datacatalog-connector/datacatalog-go-models"
	api "fybrik.io/datacatalog-connector/datacatalog-go/go"
	dbtypes "fybrik.io/datacatalog-connector/pkg/database-types"
	"fybrik.io/datacatalog-connector/pkg/utils"
	"fybrik.io/datacatalog-connector/pkg/vault"
)

const EmptyString = ""

// transform variable `tags` into an array.
// traverse `tags` and create data catalog tags in the `categoryName` category
/*func addTags(ctx context.Context, c *client.APIClient,
	categoryName string, tags interface{}, logger *zerolog.Logger) {
		tagsArr, ok := utils.InterfaceToArray(tags, logger)
		if !ok {
			logger.Warn().Msg("Malformed tag information")
			return
		}

			for i := range tagsArr {
				if tagMap, ok := tagsArr[i].(map[interface{}]interface{}); ok {
					descriptionStr := EmptyString
					description := tagMap[Description]
					if description != nil {
						descriptionStr = description.(string)
					}
					if name, ok1 := tagMap[Name]; ok1 {
						_, r, err := c.TagsApi.CreatePrimaryTag(ctx, categoryName).
							CreateTag(*client.NewCreateTag(descriptionStr, name.(string))).Execute()
						if err != nil {
							logger.Trace().Err(err).Msg("Failed to create Tag. Maybe it already exists.")
						} else {
							r.Body.Close()
						}
					} else {
						logger.Warn().Msg(fmt.Sprintf("malformed tag information. cannot cast %T to map[interface{}]interface{}", tagsArr[i]))
					}
				}
			}
}*/

func PrepareDataCatalogForFybrik(endpoint string, user string, password string,
	customization map[string]interface{}, logger *zerolog.Logger) bool {
	/* c, err := getDataCatalogClient(ctx, endpoint, user, password, logger)
	if err != nil {
		logger.Warn().Err(err).Msg(CannotLoginToDataCatalog)
		return false
	} */

	// traverse tag categories. create categories as needed.
	// within each tag category, create the specified tags

	// traverse the table custom properties, and create each

	return true
}

func (s *DataCatalogAPIService) PrepareDataCatalogForFybrik() bool {
	return PrepareDataCatalogForFybrik(s.Endpoint, s.user, s.password, s.customization, s.logger)
}

// NewDataCatalogAPIService creates a new api service.
// It is initialized base on the configuration
func NewDataCatalogAPIService(conf map[string]interface{}, customization map[string]interface{},
	logger *zerolog.Logger) *DataCatalogAPIService {
	var SleepIntervalMS int
	var NumRetries int
	var port int
	var user string
	var password string
	var vaultPluginPrefix string

	var vaultConf map[interface{}]interface{} = nil
	if vaultConfMap, ok := conf["vault"]; ok {
		vaultConf = vaultConfMap.(map[interface{}]interface{})
	}

	if value, ok := conf["datacatalog_sleep_interval"]; ok {
		SleepIntervalMS = value.(int)
	} else {
		SleepIntervalMS = DefaultSleepIntervalMS
	}

	if value, ok := conf["datacatalog_num_retries"]; ok {
		NumRetries = value.(int)
	} else {
		NumRetries = DefaultNumRetries
	}

	if value, ok := conf["datacatalog_connector_port"]; ok {
		port = value.(int)
	} else {
		port = DefaultListeningPort
	}

	if value, ok := conf["datacatalog_user"]; ok {
		user = value.(string)
	} else {
		user = DefaultDataCatalogUser
	}

	if value, ok := conf["datacatalog_password"]; ok {
		password = value.(string)
	} else {
		password = DefaultDataCatalogPassword
	}

	if vaultConf != nil {
		if value, ok := vaultConf["pluginPrefix"]; ok {
			vaultPluginPrefix = value.(string)
		} else {
			vaultPluginPrefix = DefaultVaultPluginPrefix
		}
	} else {
		vaultPluginPrefix = EmptyString
	}

	nameToDatabaseStruct := map[string]dbtypes.DatabaseType{
		Generic: dbtypes.NewGeneric(logger),
	}

	serviceTypeToConnectionType := map[string]string{
		dbtypes.CustomDatabase: Generic,
	}

	s := &DataCatalogAPIService{Endpoint: conf["datacatalog_endpoint"].(string),
		SleepIntervalMS:             SleepIntervalMS,
		NumRetries:                  NumRetries,
		NameToDatabaseStruct:        nameToDatabaseStruct,
		serviceTypeToConnectionType: serviceTypeToConnectionType,
		logger:                      logger,
		NumRenameRetries:            DefaultNumRenameRetries,
		customization:               customization,
		Port:                        port,
		user:                        user,
		password:                    password,
		vaultPluginPrefix:           vaultPluginPrefix,
	}

	s.initialized = s.PrepareDataCatalogForFybrik()

	return s
}

func getDataCatalogClient(ctx context.Context, endpoint, user, password string, logger *zerolog.Logger) (*client.APIClient, error) {
	conf := client.Configuration{Servers: client.ServerConfigurations{
		client.ServerConfiguration{
			URL:         endpoint,
			Description: "Endpoint URL",
		}},
		HTTPClient: utils.HTTPClient,
	}

	/* c := client.NewAPIClient(&conf)
	tokenStruct, r, err := c.UsersApi.LoginUserWithPwd(ctx).
		LoginRequest(*client.NewLoginRequest(user, password)).Execute()
	if err != nil {
		logger.Warn().Err(err).Msg("could not login to Data Catalog")
		return nil, err
	}
	r.Body.Close() */

	token := fmt.Sprintf("%s %s", "tokenStruct.TokenType", "tokenStruct.AccessToken")
	conf.DefaultHeader = map[string]string{"Authorization": token}
	return client.NewAPIClient(&conf), nil
}

func (s *DataCatalogAPIService) getDataCatalogClient(ctx context.Context) (*client.APIClient, error) {
	return getDataCatalogClient(ctx, s.Endpoint, s.user, s.password, s.logger)
}

// traverse database services looking for a service with identical configuration
func (s *DataCatalogAPIService) findService(ctx context.Context,
	c *client.APIClient,
	dt dbtypes.DatabaseType,
	connectionProperties map[string]interface{}) (string, string, bool) {
	s.logger.Trace().Msg("Identical database service not found")
	return EmptyString, EmptyString, false
}

func (s *DataCatalogAPIService) createDatabaseService(ctx context.Context,
	c *client.APIClient,
	createAssetRequest *models.CreateAssetRequest,
	connectionName string,
	omConfig map[string]interface{},
	omTypeName string) (string, string, error) {
	s.logger.Info().Msg(SucceededInCreatingDatabaseService)
	return "databaseService.Id", "databaseService.FullyQualifiedName", nil
}

func (s *DataCatalogAPIService) findLatestAsset(ctx context.Context, c *client.APIClient, assetID string) (bool, *client.Table) {
	var table *client.Table
	return false, table
}

func (s *DataCatalogAPIService) findIngestionPipeline(ctx context.Context, c *client.APIClient,
	ingestionPipelineName string) (string, bool) {
	return "*pipeline.Id", true
}

func (s *DataCatalogAPIService) createIngestionPipeline(ctx context.Context,
	c *client.APIClient,
	databaseServiceID string,
	ingestionPipelineName string) (string, error) {
	return "*ingestionPipeline.Id", nil
}

func (s *DataCatalogAPIService) createTable(ctx context.Context,
	c *client.APIClient,
	databaseSchemaID string,
	tableName string,
	columns []client.Column) (*client.Table, error) {
	var table *client.Table
	return table, nil
}

// enrichAsset is called after asset is created, or during an updateAsset request
// Data Catalog uses the JsonPatch format for updates
func (s *DataCatalogAPIService) enrichAsset(ctx context.Context, table *client.Table, c *client.APIClient,
	geography *string, name *string, owner *string,
	dataFormat *string,
	requestTags map[string]interface{},
	requestColumnsModels []models.ResourceColumn,
	requestColumnsAPI []api.ResourceColumn) error {
	s.logger.Info().Msg("Asset Enrichment succeeded")
	return nil
}

func (s *DataCatalogAPIService) deleteAsset(ctx context.Context, c *client.APIClient, assetID string) (int, error) {
	s.logger.Info().Msg("Asset deletion successful")
	return http.StatusOK, nil
}

// populate the values in a GetAssetResponse structure to include everything:
// credentials, name, owner, geography, dataFormat, connection information,
// tags, and columns
func (s *DataCatalogAPIService) constructAssetResponse(ctx context.Context,
	c *client.APIClient,
	table *client.Table) (*models.GetAssetResponse, error) {
	// Let's begin by finding the Database Service.
	// We need it for the connection information.

	// Once it is found, let us place the Database Service configuration
	// in the `config` variable
	var config map[string]interface{}

	ret := &models.GetAssetResponse{}
	var customProperties map[string]interface{}
	// customProperties := table.GetExtension()

	name := customProperties[Description]
	if name != nil {
		nameStr := name.(string)
		ret.ResourceMetadata.Name = &nameStr
	}

	owner := customProperties[Owner]
	if owner != nil {
		ownerStr := owner.(string)
		ret.ResourceMetadata.Owner = &ownerStr
	}

	geography := customProperties[Geography]
	if geography != nil {
		geographyStr := geography.(string)
		ret.ResourceMetadata.Geography = &geographyStr
	}

	dataFormatStr := DefaultDataFormat
	dataFormat := customProperties[DataFormat]
	if dataFormat != nil {
		dfStr, ok := dataFormat.(string)
		if ok && dfStr != "" {
			dataFormatStr = dfStr
		}
	}
	ret.Details.DataFormat = &dataFormatStr

	connectionType, ok := s.serviceTypeToConnectionType["ServiceType"]

	if !ok {
		message := "unrecognized servicetype"
		s.logger.Error().Msg(message)
		return nil, errors.New(message)
	}
	dt, found := s.NameToDatabaseStruct[connectionType]
	if !found {
		// since this connection type was not recognized, we use the generic type
		dt = s.NameToDatabaseStruct[Generic]
	}

	config, connectionType, err := dt.TranslateDataCatalogConfigToFybrikConfig("table.Name", config)
	if err != nil {
		s.logger.Error().Err(err).Msg("error in translating data catalog config to fybrik format")
		return nil, err
	}

	if s.vaultPluginPrefix != EmptyString {
		ret.Credentials = vault.GetFullSecretPath(s.vaultPluginPrefix, "DatabaseServiceFullyQualifiedName")
	}

	additionalProperties := make(map[string]interface{})
	ret.Details.Connection.Name = connectionType
	additionalProperties[connectionType] = config
	ret.Details.Connection.AdditionalProperties = additionalProperties

	/* for i := range table.Columns {
		if len(table.Columns[i].Tags) > 0 {
			tags := make(map[string]interface{})
			for _, t := range table.Columns[i].Tags {
				tags[utils.StripTag(t.TagFQN)] = "true"
			}
			ret.ResourceMetadata.Columns = append(ret.ResourceMetadata.Columns, models.ResourceColumn{Name: table.Columns[i].Name, Tags: tags})
		} else {
			ret.ResourceMetadata.Columns = append(ret.ResourceMetadata.Columns, models.ResourceColumn{Name: table.Columns[i].Name})
		}
	}

	if len(table.Tags) > 0 {
		tags := make(map[string]interface{})
		for _, s := range table.Tags {
			tags[utils.StripTag(s.TagFQN)] = "true"
		}
		ret.ResourceMetadata.Tags = tags
	} */

	s.logger.Info().Msg("Successfully constructed asset response")
	return ret, nil
}
