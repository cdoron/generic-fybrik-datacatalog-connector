// Copyright 2022 IBM Corp.
// SPDX-License-Identifier: Apache-2.0

package openapiconnectorcore

import (
	"context"
	"errors"
	"net/http"

	"github.com/rs/zerolog"

	models "fybrik.io/datacatalog-connector/datacatalog-go-models"
	api "fybrik.io/datacatalog-connector/datacatalog-go/go"
	dbtypes "fybrik.io/datacatalog-connector/pkg/database-types"
	"fybrik.io/datacatalog-connector/pkg/utils"
)

type DataCatalogAPIService struct {
	Endpoint                    string
	SleepIntervalMS             int
	NumRetries                  int
	NameToDatabaseStruct        map[string]dbtypes.DatabaseType
	serviceTypeToConnectionType map[string]string
	logger                      *zerolog.Logger
	NumRenameRetries            int
	initialized                 bool
	customization               map[string]interface{}
	Port                        int
	user                        string
	password                    string
	vaultPluginPrefix           string
}

// CreateAsset - This REST API writes data asset information to the data catalog configured in fybrik
func (s *DataCatalogAPIService) CreateAsset(ctx context.Context, //nolint
	xRequestDatacatalogWriteCred string,
	createAssetRequest *models.CreateAssetRequest) (api.ImplResponse, error) {
	if !s.initialized {
		s.initialized = s.PrepareDataCatalogForFybrik()
	}

	connectionType := createAssetRequest.Details.Connection.Name

	// check whether connectionType is one of the connection types supported by the connector
	dt, found := s.NameToDatabaseStruct[connectionType]
	if !found {
		// since this connection type was not recognized, we use the generic type
		dt = s.NameToDatabaseStruct[Generic]
	}

	c, err1 := s.getDataCatalogClient(ctx)
	if err1 != nil {
		s.logger.Error().Err(err1).Msg(CannotLoginToDataCatalog)
		return api.Response(http.StatusUnauthorized, nil), errors.New(CannotLoginToDataCatalog)
	}

	var databaseServiceID string
	var databaseServiceName string
	var err error

	// Let us begin by checking whether the database service already exists.
	// step 1: Translate the fybrik connection information to the connection information.
	//         This configuration information will later be used to create a connection
	//         (if it does not already exist)
	config, ok := utils.InterfaceToMap(createAssetRequest.Details.GetConnection().AdditionalProperties[connectionType], s.logger)
	if !ok {
		s.logger.Error().Msg(FailedToCovert)
		return api.Response(http.StatusBadRequest, nil), errors.New(FailedToCovert)
	}
	omConfig := dt.TranslateFybrikConfigToDataCatalogConfig(config, connectionType, createAssetRequest.Credentials)
	// step 2: compare the transformed connection information to that of all existing services
	databaseServiceID, databaseServiceName, found = s.findService(ctx, c, dt, omConfig)

	if !found {
		// If does not exist, let us create database service
		databaseServiceID, databaseServiceName, err =
			s.createDatabaseService(ctx, c, createAssetRequest, connectionType, omConfig, dt.DataCatalogTypeName())
		if err != nil {
			s.logger.Error().Msg("unable to create Database Service for " + dt.DataCatalogTypeName() + " connection")
			return api.Response(http.StatusBadRequest, nil), err
		}
	}

	// now that we know the of the database service, we can determine the asset name in Data Catalog
	assetID, err := dbtypes.TableFQN(dt, databaseServiceName, createAssetRequest)
	if err != nil {
		s.logger.Error().Err(err).Msg("cannot determine table FQN")
		return api.Response(http.StatusBadRequest, nil), err
	}

	// Let's check whether Data Catalog already has this asset
	found, _ = s.findLatestAsset(ctx, c, assetID)
	if found {
		s.logger.Error().Msg("Could not create asset, as asset already exists")
		return api.Response(http.StatusBadRequest, nil), errors.New("asset already exists")
	}

	// We create ingestion pipelines for all databaseServices except for 'generic' services
	if dt.DataCatalogTypeName() != CustomDatabase {
		// Asset not discovered yet
		// Let's check whether there is an ingestion pipeline we can trigger
		ingestionPipelineName := "pipeline-" + createAssetRequest.DestinationCatalogID + "." + *createAssetRequest.DestinationAssetID
		ingestionPipelineNameFull := utils.AppendStrings(databaseServiceName, ingestionPipelineName)

		// var ingestionPipelineID string nolint
		// ingestionPipelineID, found = s.findIngestionPipeline(ctx, c, ingestionPipelineNameFull)
		_, found = s.findIngestionPipeline(ctx, c, ingestionPipelineNameFull)

		if !found {
			// Let us create an ingestion pipeline
			s.logger.Info().Msg("Ingestion Pipeline not found. Creating.")
			_, _ = s.createIngestionPipeline(ctx, c, databaseServiceID, ingestionPipelineName)
		}
	}

	columns := utils.ExtractColumns(createAssetRequest.ResourceMetadata.Columns)

	tableName, err := dt.TableName(createAssetRequest)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to determine table name")
		return api.Response(http.StatusBadRequest, nil), err
	}
	table, err := s.createTable(ctx, c, "databaseSchemaID", tableName, columns)
	if err != nil {
		return api.Response(http.StatusBadRequest, nil), err
	}

	s.logger.Info().Msg("Enriching asset with additional information (e.g. tags)")
	// Now that data catalog is aware of the asset, we need to enrich it --
	// add tags to asset and to columns, and populate the custom properties
	err = s.enrichAsset(ctx, table, c,
		createAssetRequest.ResourceMetadata.Geography,
		createAssetRequest.ResourceMetadata.Name, createAssetRequest.ResourceMetadata.Owner,
		createAssetRequest.Details.DataFormat,
		createAssetRequest.ResourceMetadata.Tags,
		createAssetRequest.ResourceMetadata.Columns, nil)

	if err != nil {
		s.logger.Error().Msg("Asset enrichment failed")
		return api.Response(http.StatusBadRequest, nil), err
	}

	s.logger.Info().Msg("Asset creation and enrichment successful")

	return api.Response(http.StatusCreated, api.CreateAssetResponse{AssetID: assetID}), nil
}

// DeleteAsset - This REST API deletes data asset
func (s *DataCatalogAPIService) DeleteAsset(ctx context.Context, xRequestDatacatalogCred string,
	deleteAssetRequest *api.DeleteAssetRequest) (api.ImplResponse, error) {
	if !s.initialized {
		s.initialized = s.PrepareDataCatalogForFybrik()
	}

	c, err := s.getDataCatalogClient(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg(CannotLoginToDataCatalog)
		return api.Response(http.StatusUnauthorized, nil), errors.New(CannotLoginToDataCatalog)
	}

	errorCode, err := s.deleteAsset(ctx, c, deleteAssetRequest.AssetID)

	if err != nil {
		s.logger.Info().Msg("Asset deletion failed")
		return api.Response(errorCode, nil), err
	}

	s.logger.Info().Msg("Asset deletion successful")
	return api.Response(http.StatusOK, api.DeleteAssetResponse{}), nil
}

/**
 * GetAssetInfo - return data asset information from the data catalog
 * Paremeters:
 * - xRequestDatacatalogCred - credential information related to catalog from which the asset information is retrieved
 * - getAssetRequest - structure which includes 'assetID' and 'operationType' (which is always 'read')
 *
 * Return values:
 * - GetAssetResponse object, which includes:
 *   - credentials - vault plugin path where the data credentials are stored
 *   - resourceMetadata:
 *     - name - name of the resource (optional)
 *     - owner - owner of the resource (optional)
 *     - geography - geography of the resource (optional)
 *     - tags - map of tags associated with the asset, e.g. 'Purpose.finance: true' (optional)
 *     - columns - list of columns. each column must include a name. a column may also contain a map of tags (optional)
 *   - details:
 *     - dataFormat - format in which the data is being read/written by the workload (optional)
 *     - connection:
 *       - name: name of the connection to the data source, e.g. 'mysql'
 *       - additional properties: map. for instance, if 'name' is 'mysql', you need a 'mysql' property with a
 *                                value which is a map containing all connection information for data source. In
 *                                the case of 'mysql', that information must include 'host', 'port', 'database' and 'table'
 * - error object, if error was encountered, or nil otherwise
 */
func (s *DataCatalogAPIService) GetAssetInfo(ctx context.Context, xRequestDatacatalogCred string,
	getAssetRequest *api.GetAssetRequest) (api.ImplResponse, error) {
	if !s.initialized {
		s.initialized = s.PrepareDataCatalogForFybrik()
	}

	c, err := s.getDataCatalogClient(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg(CannotLoginToDataCatalog)
		return api.Response(http.StatusUnauthorized, nil), errors.New(CannotLoginToDataCatalog)
	}

	assetID := getAssetRequest.AssetID

	found, table := s.findLatestAsset(ctx, c, assetID)
	if !found {
		s.logger.Error().Msg("Asset not found")
		return api.Response(http.StatusNotFound, nil), errors.New("asset not found")
	}

	assetResponse, err := s.constructAssetResponse(ctx, c, table)
	if err != nil {
		s.logger.Error().Msg("Construction of Asset Response failed")
		return api.Response(http.StatusBadRequest, nil), err
	}

	s.logger.Info().Msg("GetAssetInfo successful")
	return api.Response(http.StatusOK, assetResponse), nil
}

// UpdateAsset - This REST API updates data asset information in the data catalog configured in fybrik
func (s *DataCatalogAPIService) UpdateAsset(ctx context.Context, xRequestDatacatalogUpdateCred string,
	updateAssetRequest *api.UpdateAssetRequest) (api.ImplResponse, error) {
	if !s.initialized {
		s.initialized = s.PrepareDataCatalogForFybrik()
	}

	c, err := s.getDataCatalogClient(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg(CannotLoginToDataCatalog)
		return api.Response(http.StatusUnauthorized, nil), errors.New(CannotLoginToDataCatalog)
	}
	assetID := updateAssetRequest.AssetID

	found, table := s.findLatestAsset(ctx, c, assetID)
	if !found {
		s.logger.Error().Msg(AssetNotFound)
		return api.Response(http.StatusNotFound, nil), errors.New(AssetNotFound)
	}

	err = s.enrichAsset(ctx, table, c, nil, &updateAssetRequest.Name, &updateAssetRequest.Owner, nil,
		updateAssetRequest.Tags, nil, updateAssetRequest.Columns)
	if err != nil {
		s.logger.Error().Msg("asset enrichment failed")
		return api.Response(http.StatusBadRequest, nil), err
	}

	s.logger.Info().Msg("UpdateAsset successful")
	return api.Response(http.StatusOK, api.UpdateAssetResponse{Status: "Asset update operation successful"}), nil
}
