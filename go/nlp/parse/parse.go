package parse

// Parse performs parsing on Baleen documents transmitted via Ensign stream

import (
	"fmt"

	"github.com/anaskhan96/soup"
	"github.com/cdipaolo/sentiment"

	prose "github.com/jdkato/prose/v2"
	events "github.com/rotationalio/baleen/events"
)

// ParseResponse takes in an Ensign Baleen Document and calls ParseHTML and ParseString
// to return a map of entities to entity types
// TODO: update so that we don't overwrite entity keys if they occur multiple times
func ParseResponse(document *events.Document, model sentiment.Models) (entities map[string]string, avgSentiment float32, err error) {
	var titleEnts map[string]string
	if titleEnts, err = ParseString(document.Title); err != nil {
		fmt.Println(err)
	}
	var articleEnts map[string]string
	if articleEnts, avgSentiment, err = ParseHTML(document.Content, model); err != nil {
		fmt.Println(err)
	}
	entities = make(map[string]string)
	for tEnt, tTag := range titleEnts {
		entities[tEnt] = tTag
	}
	for aEnt, aTag := range articleEnts {
		entities[aEnt] = aTag
	}

	return entities, avgSentiment, err
}

// ParseHTML parses the html that baleen sends back in the doc.Content
// returning a map of entities to entity types
func ParseHTML(content []byte, model sentiment.Models) (entities map[string]string, avgSentiment float32, err error) {
	// allocate empty entity map
	entities = make(map[string]string)

	// parse html
	doc := soup.HTMLParse(string(content))
	paras := doc.FindAll("p")

	var sentimentScores []uint8

	// extract the entities and sentiment scores from the paragraphs
	for _, p := range paras {

		// Get the sentiment score for each paragraph
		analysis := model.SentimentAnalysis(p.Text(), sentiment.English)
		sentimentScores = append(sentimentScores, analysis.Score)

		// parse the entities
		var parsed *prose.Document
		if parsed, err = prose.NewDocument(p.Text()); err != nil {
			return nil, avgSentiment, err
		}
		for _, ent := range parsed.Entities() {
			// add entities to map
			entities[ent.Text] = ent.Label
		}
	}

	// Get the average sentiment score across all the paragraphs
	var total float32 = 0
	for _, s := range sentimentScores {
		total += float32(s)
	}
	avgSentiment = total / float32(len(sentimentScores))

	return entities, avgSentiment, nil
}

// ParseString parses the string that baleen sends back in the doc.Title
// returning a map of entities to entity types
func ParseString(title string) (entities map[string]string, err error) {
	// allocate empty entity map
	entities = make(map[string]string)

	// extract entities
	var parsed *prose.Document
	if parsed, err = prose.NewDocument(title); err != nil {
		return nil, err
	}
	for _, ent := range parsed.Entities() {
		// add entities to map
		entities[ent.Text] = ent.Label
	}

	return entities, nil

}
