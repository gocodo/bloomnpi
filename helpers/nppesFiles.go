package helpers

import (
	"io/ioutil"
	"net/http"
	"regexp"
)

var ignore = regexp.MustCompile(`<\!\-\-(.|\n)*?\-\-\>`)
var weeklyRegex = regexp.MustCompile(`(NPPES_Data_Dissemination_\d+_\d+_Weekly).zip`)
var monthlyRegex = regexp.MustCompile(`(NPPES_Data_Dissemination_[a-zA-Z]+_\d+).zip`)

func FilesAvailable() (string, []string, error) {
	resp, err := http.Get("http://download.cms.gov/nppes/NPI_Files.html")
	if err != nil {
		return "", nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return "", nil, err
	}

	bodyS := string(body)

	bodyS = ignore.ReplaceAllString(bodyS, "")
	monthlyMatches := monthlyRegex.FindStringSubmatch(bodyS)
	weeklyMatches := weeklyRegex.FindAllStringSubmatch(bodyS, -1)

	var monthly string
	if len(monthlyMatches) > 0 {
		monthly = monthlyMatches[1]
	} else {
		monthly = ""
	}

	weekly := []string{}
	for _, weeklyMatch := range weeklyMatches {
		if len(weeklyMatch) > 0 {
			weekly = append(weekly, weeklyMatch[1])
		}
	}

	return monthly, weekly, nil
}
