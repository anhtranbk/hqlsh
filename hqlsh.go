package main

import (
	"fmt"
	"os"
	tb "github.com/olekukonko/tablewriter"
	"net/http"
	"encoding/json"
	"io/ioutil"
	b64 "encoding/base64"
	"encoding/binary"
	"strconv"
	"bytes"
)

const BASE_URL = "http://10.5.36.102:8080"
// const BASE_URL = "http://localhost:9999"

func printTable(headers []string, data [][]string) {
	fmt.Println()

	table := tb.NewWriter(os.Stdout)
	table.SetHeader(headers)
	table.SetBorder(false)                                // Set Border to false
	table.AppendBulk(data)                                // Add Bulk Data
	table.Render()

	fmt.Printf("\n(%d rows)\n", len(data))
}

func tryDecodeBytes(b []byte) string {
	if len(b) == 8 {
		var n int64
		err := binary.Read(bytes.NewReader(b), binary.BigEndian, &n)
		if err == nil {
			return strconv.FormatInt(n, 10)
		}
	}
	return string(b)
}

func decode(str string) string {
	raw, _ := b64.StdEncoding.DecodeString(str)
	return tryDecodeBytes(raw)
}

func initEmptySliceString(size int) []string {
	sl := make([]string, size)
	for i := 0; i < size; i++ {
		sl[i] = ""
	}
	return sl
}

func parseToTable(m map[string]interface{}) ([]string, [][]string) {
	hMap := map[string]int{"pk": 0}
	data := make([][]string, 0)

	for _, r := range m["Row"].([]interface{}) {
		row := r.(map[string]interface{})
		key := decode(row["key"].(string))
		cells := initEmptySliceString(len(hMap))
		cells[0] = string(key[:])

		for _, c := range row["Cell"].([]interface{}) {
			cell := c.(map[string]interface{})
			column := decode(cell["column"].(string))
			value := decode(cell["$"].(string))

			if order, ok := hMap[column]; ok {
				cells[order] = value
			} else {
				order = len(hMap)
				hMap[column] = order
				cells = append(cells, value)
			}
		}
		data = append(data, cells)
	}

	headers := initEmptySliceString(len(hMap))
	for k, _ := range hMap {
		headers[hMap[k]] = k
	}

	return headers, data
}

func parseFromJson(text string) map[string]interface{} {
	var f interface{}
	err := json.Unmarshal([]byte(text), &f)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error parsing JSON: ", err)
		return nil
	}

	// JSON object parses into a map with string keys
	return f.(map[string]interface{})
}

func fetchDataFromUrl(url string) map[string]interface{} {
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed with status code %s", resp.Status)
		return nil
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		fmt.Println("Get response body error", err)
		return nil
	}

	// fmt.Println(string(body))
	return parseFromJson(string(body))
}

func processGetRequest(tableName string, row string) map[string]interface{} {
	url := fmt.Sprintf("%s/%s/%s", BASE_URL, tableName, row)
	return fetchDataFromUrl(url)
}

func processScanRequest(tableName string, prefix string, limit int32) map[string]interface{} {
	body := fmt.Sprintf(`
	<Scanner batch="%d">
    <filter> { "type": "PrefixFilter", "value": "%s" } </filter>
    </Scanner>`, limit, prefix)

	url := fmt.Sprintf("%s/%s/scanner", BASE_URL, tableName)
	req, err := http.NewRequest("PUT", url, bytes.NewReader([]byte(body)))
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "text/xml")

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed with status code %s", resp.Status)
		return nil
	}

	location := resp.Header.Get("Location")
	defer resp.Body.Close()

	return fetchDataFromUrl(location)
}

func main() {
	// tb := "g:edge"
	// row := "01AzdbYnXcj_zIXbgaH3mKzMFR0=|4655833637332710347|fb.com|o|735526436607171"
	// if m := processGetRequest(tb, row); m != nil {
	// 	printTable(parseToTable(m))
	// }

	if m := processScanRequest("g:vertex", "", 100); m != nil {
		printTable(parseToTable(m))
	}
}
