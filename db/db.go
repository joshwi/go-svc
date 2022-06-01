package db

import (
	"fmt"
	"regexp"

	"github.com/joshwi/go-pkg/logger"
	"github.com/joshwi/go-pkg/utils"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var regexp_1 = regexp.MustCompile(`"`)

func Connect(url string, username string, password string) neo4j.Driver {

	Neo4jConfig := func(conf *neo4j.Config) {}

	driver, err := neo4j.NewDriver(url, neo4j.BasicAuth(username, password, ""), Neo4jConfig)
	if err != nil {
		logger.Logger.Error().Str("url", url).Str("status", "Failed").Err(err).Msg("Connect")
		return nil
	}
	logger.Logger.Info().Str("url", url).Str("status", "Success").Msg("Connect")

	return driver
}

func RunCypher(session neo4j.Session, query string) ([][]utils.Tag, error) {

	output := [][]utils.Tag{}

	// defer session.Close()

	result, err := session.Run(query, map[string]interface{}{})
	if err != nil {
		logger.Logger.Error().Str("status", "Failed").Err(err).Msg("RunCypher")
		return output, err
	}

	for result.Next() {
		entry := []utils.Tag{}
		keys := result.Record().Keys
		for n := 0; n < len(keys); n++ {
			value := fmt.Sprintf("%v", result.Record().GetByIndex(n))
			input := utils.Tag{
				Name:  keys[n],
				Value: value,
			}
			entry = append(entry, input)
		}
		output = append(output, entry)
	}

	return output, nil
}

func GetNode(session neo4j.Session, node string, query string, limit int, properties []string) ([]map[string]string, error) {

	output := []map[string]string{}

	cypher := `MATCH (n: ` + node + `) `

	if len(query) > 0 {
		cypher += `WHERE ` + query + ` RETURN `
	} else {
		cypher += ` RETURN `
	}

	if len(properties) > 0 {
		for n, item := range properties {
			if n == 0 {
				cypher += fmt.Sprintf(`n.%v as %v`, item, item)
			} else {
				cypher += fmt.Sprintf(`, n.%v as %v`, item, item)
			}

		}
	} else {
		cypher += `n as n`
	}

	if limit > 0 {
		cypher += fmt.Sprintf(` LIMIT %v`, limit)
	}

	result, err := session.Run(cypher, map[string]interface{}{})
	if err != nil {
		logger.Logger.Error().Str("node", node).Str("query", query).Err(err).Msg("GetNode")
		return output, err
	}

	if len(properties) > 0 {
		for result.Next() {
			entry := map[string]string{}
			keys := result.Record().Keys
			for n := 0; n < len(keys); n++ {
				value := fmt.Sprintf("%v", result.Record().GetByIndex(n))
				entry[keys[n]] = value
			}
			output = append(output, entry)
		}
	} else {
		for result.Next() {
			record := map[string]string{}
			temp, _ := result.Record().Get("n")
			node_props := temp.(neo4j.Node).Props
			for k, v := range node_props {
				record[k] = v.(string)
			}
			output = append(output, record)
		}
	}

	logger.Logger.Info().Str("node", node).Str("query", query).Msg("GetNode")

	return output, nil
}

func PostNode(session neo4j.Session, node string, label string, properties []utils.Tag) error {

	cypher := `CREATE (n: ` + node + ` { label: "` + label + `" })`

	for _, item := range properties {
		cypher += ` SET n.` + item.Name + ` = "` + regexp_1.ReplaceAllString(item.Value, "\\'") + `"`
	}

	_, err := session.Run(cypher, map[string]interface{}{})
	if err != nil {
		logger.Logger.Error().Str("node", node).Str("label", label).Err(err).Msg("PostNode")
		return err
	}

	logger.Logger.Info().Str("node", node).Str("label", label).Msg("PostNode")

	return nil
}

func PutNode(session neo4j.Session, node string, label string, properties []utils.Tag) error {

	cypher := `MERGE (n: ` + node + ` { label: "` + label + `" })`

	for _, item := range properties {
		cypher += ` SET n.` + item.Name + ` = "` + regexp_1.ReplaceAllString(item.Value, "\\'") + `"`
	}

	_, err := session.Run(cypher, map[string]interface{}{})
	if err != nil {
		logger.Logger.Error().Str("node", node).Str("label", label).Err(err).Msg("PutNode")
		return err
	}

	logger.Logger.Info().Str("node", node).Str("label", label).Msg("PutNode")

	return nil

}
