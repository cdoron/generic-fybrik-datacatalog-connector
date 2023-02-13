/*
 * Data Catalog Service - Asset Details
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * API version: 1.0.0
 * Generated by: OpenAPI Generator (https://openapi-generator.tech)
 */

package openapi

// ResourceDetails - ResourceDetails includes asset connection details
type ResourceDetails struct {
	Connection Connection `json:"connection"`

	DataFormat string `json:"dataFormat,omitempty"`
}

// AssertResourceDetailsRequired checks if the required fields are not zero-ed
func AssertResourceDetailsRequired(obj ResourceDetails) error {
	elements := map[string]interface{}{
		"connection": obj.Connection,
	}
	for name, el := range elements {
		if isZero := IsZeroValue(el); isZero {
			return &RequiredError{Field: name}
		}
	}

	return nil
}

// AssertRecurseResourceDetailsRequired recursively checks if required fields are not zero-ed in a nested slice.
// Accepts only nested slice of ResourceDetails (e.g. [][]ResourceDetails), otherwise ErrTypeAssertionError is thrown.
func AssertRecurseResourceDetailsRequired(objSlice interface{}) error {
	return AssertRecurseInterfaceRequired(objSlice, func(obj interface{}) error {
		aResourceDetails, ok := obj.(ResourceDetails)
		if !ok {
			return ErrTypeAssertionError
		}
		return AssertResourceDetailsRequired(aResourceDetails)
	})
}
