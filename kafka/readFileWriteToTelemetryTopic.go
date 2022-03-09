package kafka

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

func ReadFileWriteToTelemetryTopic() {
	// This is the last step in the process.
	// We consolidate the log into json and produce it to
	// TelemetryTopic in Kafka

	// Open our jsonFile
	jsonFile, err := os.Open("log.txt")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened log.txt")
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]interface{}
	json.Unmarshal([]byte(byteValue), &result)

	fmt.Println(result)

}
