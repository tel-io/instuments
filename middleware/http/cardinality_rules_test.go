package http

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCardinalityGrouper(t *testing.T) {
	gP, errP := NewRulesGrouper([]string{
		"/:AA/b01/c01/:DD",
		"/a02/:BB/c02/:DD",
		"/a03/b03/:CC/:DD",
		"/a04/b04/c04/:DD",
		"/:AA/b05/c05/d05",
		"/:AA/:BB/c06/d06",
		"/:AA/b07/:CC/d07",
		"/:AA/b08/c08/:DD",
		"/a09/:BB/:CC/d09",
		"/a10/:BB/:CC/:DD",
		"/:AA/:BB/:CC/d11",
	})
	assert.NoError(t, errP)

	tests := map[string]string{
		"/a01/b01/c01/d01": "/:AA/b01/c01/:DD",
		"/a02/b02/c02/d02": "/a02/:BB/c02/:DD",
		"/a03/b03/c03/d03": "/a03/b03/:CC/:DD",
		"/a04/b04/c04/d04": "/a04/b04/c04/:DD",
		"/a05/b05/c05/d05": "/:AA/b05/c05/d05",
		"/a06/b06/c06/d06": "/:AA/:BB/c06/d06",
		"/a07/b07/c07/d07": "/:AA/b07/:CC/d07",
		"/a08/b08/c08/d08": "/:AA/b08/c08/:DD",
		"/a09/b09/c09/d09": "/a09/:BB/:CC/d09",
		"/a10/b10/c10/d10": "/a10/:BB/:CC/:DD",
		"/a11/b11/c11/d11": "/:AA/:BB/:CC/d11",

		"/a12/b12/c12/d12": "/a12/b12/c12/d12",
		"/a13/b13/c13":     "/a13/b13/c13",
		"/a14/b14":         "/a14/b14",
		"/a15":             "/a15",

		"/a16/b16/c16/d16": "/a16/b16/c16/d16",
		"/a17/b17/c17":     "/a17/b17/c17",
		"/a18/b18":         "/a18/b18",
		"/a19":             "/a19",
	}

	var list = CardinalityGrouperList{gP}
	for url, exp := range tests {
		assert.Equal(t, exp, list.Apply(url))
	}
}

func TestCardinalityGrouperInvalidRules(t *testing.T) {
	var errP error

	_, errP = NewRulesGrouper(nil)
	assert.Error(t, errP)

	_, errP = NewRulesGrouper([]string{})
	assert.Error(t, errP)

	_, errP = NewRulesGrouper([]string{""})
	assert.Error(t, errP)

	_, errP = NewRulesGrouper([]string{"/"})
	assert.Error(t, errP)

	_, errP = NewRulesGrouper([]string{"//"})
	assert.Error(t, errP)

	_, errP = NewRulesGrouper([]string{"/main"})
	assert.Error(t, errP)

	_, errP = NewRulesGrouper([]string{strings.Repeat("/:x", maxRulePartCount)})
	assert.Error(t, errP)

	_, err := NewRulesGrouper(make([]string, maxRuleCount))
	assert.Error(t, err)
}

func TestCardinalityGrouperRulesByLen1(t *testing.T) {
	gP, errP := NewRulesGrouper([]string{
		"/:AA/:BB/:CC/:DD",
		"/:AA/:BB/:CC",
		"/:AA/:BB",
		"/:AA",
	})
	assert.NoError(t, errP)

	tests := map[string]string{
		"/a12/b12/c12/d12": "/:AA/:BB/:CC/:DD",
		"/a13/b13/c13":     "/:AA/:BB/:CC",
		"/a14/b14":         "/:AA/:BB",
		"/a15":             "/:AA",
	}

	var list = CardinalityGrouperList{gP}
	for url, exp := range tests {
		assert.Equal(t, exp, list.Apply(url))
	}
}

func TestCardinalityGrouperRulesByLen2(t *testing.T) {
	gP, errP := NewRulesGrouper([]string{
		"/a10/:BB/c10",
		"/a11/b11/:CC",
		"/a12/:BB",
		"/:AA/b13/c13",
		"/:AA/b14",
		"/:AA",
	})
	assert.NoError(t, errP)

	tests := map[string]string{
		"/a10/b10/c10": "/a10/:BB/c10",
		"/a11/b11/c11": "/a11/b11/:CC",
		"/a12/b12":     "/a12/:BB",
		"/a13/b13/c13": "/:AA/b13/c13",
		"/a14/b14":     "/:AA/b14",
		"/a15":         "/:AA",
	}

	var list = CardinalityGrouperList{gP}
	for url, exp := range tests {
		assert.Equal(t, exp, list.Apply(url))
	}
}
