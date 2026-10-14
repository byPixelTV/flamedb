package query

import (
	"fmt"
	"strconv"
	"strings"
)

func isRankingCommand(cmd string) bool {
	return cmd == "RANK" || cmd == "SCORE" || cmd == "AROUND" || cmd == "LEADERBOARD_COUNT"
}
func parseRanking(tokens []string) (*Query, error) {
	cmd := strings.ToUpper(tokens[0])
	tail := 2
	if len(tokens) < 2 {
		return nil, fmt.Errorf("%s requires metric", cmd)
	}
	entity := ""
	radius := 0
	if cmd != "LEADERBOARD_COUNT" {
		if len(tokens) < 3 || tokenValue(tokens[2]) == "" {
			return nil, fmt.Errorf("%s requires entity", cmd)
		}
		entity = tokenValue(tokens[2])
		tail = 3
	}
	if cmd == "AROUND" {
		if len(tokens) < 4 {
			return nil, fmt.Errorf("AROUND requires radius")
		}
		var err error
		radius, err = strconv.Atoi(tokens[3])
		if err != nil || radius < 0 || radius > 100000 {
			return nil, fmt.Errorf("invalid radius")
		}
		tail = 4
	}
	q, err := Parse("LEADERBOARD " + tokens[1] + " " + strings.Join(tokens[tail:], " "))
	if err != nil {
		return nil, err
	}
	q.Type = QueryType(cmd)
	q.EntityID = entity
	q.Radius = radius
	return q, nil
}
