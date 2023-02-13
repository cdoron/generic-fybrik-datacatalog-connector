/*
 * Data Catalog Service - Asset Details
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * API version: 1.0.0
 * Generated by: OpenAPI Generator (https://openapi-generator.tech)
 */

package openapi

type CreateAssetResponse struct {

	// The ID of the created asset based on the source asset information given in CreateAssetRequest object
	AssetID string `json:"assetID"`
}

// AssertCreateAssetResponseRequired checks if the required fields are not zero-ed
func AssertCreateAssetResponseRequired(obj CreateAssetResponse) error {
	elements := map[string]interface{}{
		"assetID": obj.AssetID,
	}
	for name, el := range elements {
		if isZero := IsZeroValue(el); isZero {
			return &RequiredError{Field: name}
		}
	}

	return nil
}

// AssertRecurseCreateAssetResponseRequired recursively checks if required fields are not zero-ed in a nested slice.
// Accepts only nested slice of CreateAssetResponse (e.g. [][]CreateAssetResponse), otherwise ErrTypeAssertionError is thrown.
func AssertRecurseCreateAssetResponseRequired(objSlice interface{}) error {
	return AssertRecurseInterfaceRequired(objSlice, func(obj interface{}) error {
		aCreateAssetResponse, ok := obj.(CreateAssetResponse)
		if !ok {
			return ErrTypeAssertionError
		}
		return AssertCreateAssetResponseRequired(aCreateAssetResponse)
	})
}
