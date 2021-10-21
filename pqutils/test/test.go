package test

import (
	"encoding/json"
	"github.com/gbnyc26/configurator"
)

type testType struct {
	Id         int    `json:"id" sql:"id,primarykey,serial"`
	FirstName  string `json:"firstName" sql:"first_name"`
	MiddleName string `json:"middleName" sql:"middle_name"`
	LastName   string `json:"lastName" sql:"last_name"`
}

func (p *testType) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

type testConfig struct {
	DbUrl string `env:"TEST_DB_URL"`
}

func configureTest() (testConfig, error) {
	var config testConfig
	err := configurator.SetEnvFromFile("test.env")
	if err != nil {
		return testConfig{}, nil
	}
	err = configurator.ParseEnvConfig(&config)
	if err != nil {
		return testConfig{}, err
	}

	return config, nil
}
