// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package openapi

import (
	"encoding/json"
)

// DatabaseService struct for DatabaseService
type DatabaseService struct {
}

// NewDatabaseService instantiates a new DatabaseService object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewDatabaseService() *DatabaseService {
	this := DatabaseService{}
	return &this
}

// NewDatabaseServiceWithDefaults instantiates a new DatabaseService object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewDatabaseServiceWithDefaults() *DatabaseService {
	this := DatabaseService{}
	return &this
}


func (o DatabaseService) MarshalJSON() ([]byte, error) {
	toSerialize := map[string]interface{}{}
	return json.Marshal(toSerialize)
}

type NullableDatabaseService struct {
	value *DatabaseService
	isSet bool
}

func (v NullableDatabaseService) Get() *DatabaseService {
	return v.value
}

func (v *NullableDatabaseService) Set(val *DatabaseService) {
	v.value = val
	v.isSet = true
}

func (v NullableDatabaseService) IsSet() bool {
	return v.isSet
}

func (v *NullableDatabaseService) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableDatabaseService(val *DatabaseService) *NullableDatabaseService {
	return &NullableDatabaseService{value: val, isSet: true}
}

func (v NullableDatabaseService) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableDatabaseService) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}
