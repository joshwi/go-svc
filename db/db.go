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
