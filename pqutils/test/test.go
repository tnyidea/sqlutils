package test

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
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
	DbUrl string
}

func configureTest() (testConfig, error) {
	// Parse the env file
	// Expected format is ENV_VAR=value, one per line
	file, err := os.Open("test.env")
	if err != nil {
		return testConfig{}, err
	}
	defer func() {
		_ = file.Close()
	}()

	var config testConfig
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err = scanner.Err(); err != nil {
			return testConfig{}, err
		}

		// Only handle items of form VARIABLE=value
		tokens := strings.Split(scanner.Text(), "=")
		if len(tokens) == 2 {
			if tokens[0] == "TEST_DB_URL" {
				config.DbUrl = tokens[1]
			}
		}
	}

	return config, nil
}
