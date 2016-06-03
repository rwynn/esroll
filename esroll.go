package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/dustin/go-humanize"
	elastigo "github.com/mattbaird/elastigo/lib"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

var rollUnits = map[string]bool{
	"bytes":   true,
	"minutes": true,
	"hours":   true,
	"days":    true,
	"months":  true,
	"years":   true,
}

const ExampleConfig string = `curl -XPUT localhost:9200/esroll/config/snowball -d '{
	"targetIndex": "snowball",
	"rollUnit": "minutes",
	"rollIncrement": 3,
	"searchAliases": 4,
	"searchSuffix": "search",
	"deleteOld": false,
	"closeOld": true,
	"optimizeOnRoll": true,
	"optimizeMaxSegments": 2,
	"settings": {
		"index": {
			"number_of_replicas": 5
		}
	}
}'`

type Configs []IndexConfig
type ByIndexAge []Index

func (a ByIndexAge) Len() int           { return len(a) }
func (a ByIndexAge) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByIndexAge) Less(i, j int) bool { return a[i].Name < a[j].Name }

type EsRollConfig struct {
	ElasticUrl     string
	ElasticPemFile string
	Daemon         bool
}

type Index struct {
	Name    string
	Status  string
	PriSize int64
}

type IndexConfig struct {
	Id                      string
	Settings                map[string]interface{} `json:"settings",omitempty`
	SettingsOnRoll          map[string]interface{} `json:"settingsOnRoll",omitempty`
	TargetIndex             string                 `json:"targetIndex"`
	RollIncrement           int                    `json:"rollIncrement"`
	RollUnit                string                 `json:"rollUnit"`
	RollSize                string                 `json:"rollSize",omitempty`
	IndexesToAliasForSearch int                    `json:"searchAliases",omitempty`
	SearchSuffix            string                 `json:"searchSuffix",omitempty`
	DeleteOldIndexes        bool                   `json:"deleteOld",omitempty`
	CloseOldIndexes         bool                   `json:"closeOld",omitempty`
	OptimizeOnRoll          bool                   `json:"optimizeOnRoll",omitempty`
	OptimizeMaxSegments     int                    `json:"optimizeMaxSegments",omitempty`
}

func GetConfigs(conn *elastigo.Conn) (Configs, error) {
	res, err := elastigo.Search("esroll").Type("config").Result(conn)
	if err != nil {
		return nil, err
	}
	if res.Hits.Total == 0 {
		return nil, errors.New("configuration documents not found")
	}
	var configs Configs
	for _, hit := range res.Hits.Hits {
		var configData IndexConfig
		if err := json.Unmarshal(*hit.Source, &configData); err != nil {
			return nil, err
		}
		configData.Id = hit.Id
		configData.SetDefaults()
		if err := configData.Validate(); err != nil {
			fmt.Println(err)
		} else {
			configs = append(configs, configData)
		}
	}
	return configs, nil
}

func PutSettings(conn *elastigo.Conn, index string, settings map[string]interface{}) error {
	url := fmt.Sprintf("/%s/_settings", index)
	body, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	_, err = conn.DoCommand("PUT", url, nil, body)
	if err != nil {
		return err
	}
	return nil
}

func UpdateAliases(conn *elastigo.Conn, body string) error {
	_, err := conn.DoCommand("POST", "/_aliases", nil, body)
	return err
}

func UpdateAliasAction(action string, index string, alias string) string {
	return fmt.Sprintf(`{ "%s" : { "index" : "%s", "alias" : "%s" } }`, action, index, alias)
}

func UpdateAliasActions(actions []string) string {
	var request bytes.Buffer
	request.WriteString(`{ "actions": [ `)
	request.WriteString(strings.Join(actions, ", "))
	request.WriteString(` ] }`)
	return request.String()
}

func (config *IndexConfig) Validate() error {
	if config.Id == "" {
		return errors.New("configuration id is missing")
	}
	if config.IndexesToAliasForSearch < 0 {
		return errors.New("searchIndexes must be greater than or equal to 0")
	}
	if config.RollUnit == "" {
		return errors.New("configuration id[" + config.Id + "] is invalid. rollUnit is required.")
	} else if rollUnits[config.RollUnit] == false {
		return errors.New("configuration id[" + config.Id + "] is invalid. rollUnit must be one of minutes, hours, days, months, or years.")
	}
	if config.OptimizeMaxSegments < 0 {
		return errors.New("optimizeMaxSegments must be greater than or equal to 0")
	}
	if config.RollUnit == "bytes" {
		if config.RollSize == "" {
			return errors.New("rollSize is required if the rollUnit is bytes")
		} else {
			maxBytes, err := humanize.ParseBytes(config.RollSize)
			if err != nil {
				return err
			} else if maxBytes == 0 {
				return errors.New("rollSize must be greater than 0 bytes")
			}
		}
	}
	return nil
}

func (config *IndexConfig) SetDefaults() {
	if config.TargetIndex == "" {
		config.TargetIndex = config.Id
	}
	if config.SearchSuffix == "" {
		config.SearchSuffix = "search"
	}
	if config.IndexesToAliasForSearch == 0 {
		config.IndexesToAliasForSearch = 2
	}
}

func (config *IndexConfig) IndexSuffixFormat() string {
	if config.RollUnit == "bytes" {
		return "2006-01-02-15-04-05"
	} else if config.RollUnit == "minutes" {
		return "2006-01-02-15-04"
	} else if config.RollUnit == "hours" {
		return "2006-01-02-15"
	} else if config.RollUnit == "days" {
		return "2006-01-02"
	} else if config.RollUnit == "months" {
		return "2006-01"
	} else {
		return "2006"
	}
}

func (config *IndexConfig) NextIndex(t time.Time) string {
	suffix := t.Format(config.IndexSuffixFormat())
	return config.TargetIndex + "_" + suffix
}

func (config *IndexConfig) IndexSize(conn *elastigo.Conn) (int64, error) {
	catIndexInfo := conn.GetCatIndexInfo(config.TargetIndex)
	infoSize := len(catIndexInfo)
	if infoSize == 0 {
		return 0, errors.New("Unable to find index in size calculation")
	} else if infoSize > 1 {
		return 0, errors.New("Too many indices returned in size calculation")
	} else {
		index := catIndexInfo[0]
		return index.Store.PriSize, nil
	}
}

func (config *IndexConfig) HasRoom(conn *elastigo.Conn) (bool, error) {
	if exists, _ := conn.ExistsIndex(config.TargetIndex, "", nil); !exists {
		return false, nil
	} else {
		priSize, err := config.IndexSize(conn)
		if err != nil {
			return true, err
		} else {
			maxBytes, err := humanize.ParseBytes(config.RollSize)
			if err != nil {
				return true, err
			} else {
				return priSize < int64(maxBytes), nil
			}
		}
	}
}

func (config *IndexConfig) Roll(conn *elastigo.Conn, t time.Time) error {
	nextIndex := config.NextIndex(t)
	if config.RollsOnSize() {
		room, err := config.HasRoom(conn)
		if err != nil {
			return err
		} else if room {
			return nil
		}
	} else if exists, _ := conn.ExistsIndex(nextIndex, "", nil); exists {
		return nil
	}
	settings := make(map[string]interface{})
	if config.Settings != nil {
		settings = config.Settings
	}
	oldIndexes := config.OldIndexNames(conn)
	if _, err := conn.CreateIndexWithSettings(nextIndex, settings); err != nil {
		return err
	}
	var cleanup, actions, optimizes []string
	var searchSuffix string = "_" + config.SearchSuffix
	actions = append(actions, UpdateAliasAction("add", nextIndex, config.TargetIndex))
	actions = append(actions, UpdateAliasAction("add", nextIndex, config.TargetIndex+searchSuffix))
	searchIndexes := 1 + len(oldIndexes)
	for i, oldIndex := range oldIndexes {
		actions = append(actions, UpdateAliasAction("remove", oldIndex.Name, config.TargetIndex))
		retire := (searchIndexes - 1) >= config.IndexesToAliasForSearch
		if retire {
			actions = append(actions, UpdateAliasAction("remove", oldIndex.Name, config.TargetIndex+searchSuffix))
			searchIndexes = searchIndexes - 1
			if config.DeleteOldIndexes {
				cleanup = append(cleanup, oldIndex.Name)
			} else if config.CloseOldIndexes && oldIndex.Status == "open" {
				cleanup = append(cleanup, oldIndex.Name)
			}
		} else if config.OptimizeOnRoll || (config.SettingsOnRoll != nil) {
			if i == (len(oldIndexes) - 1) {
				optimizes = append(optimizes, oldIndex.Name)
			}
		}
	}
	if err := UpdateAliases(conn, UpdateAliasActions(actions)); err != nil {
		return err
	}
	if len(cleanup) > 0 {
		if config.DeleteOldIndexes {
			clean := strings.Join(cleanup, ",")
			if _, err := conn.DeleteIndex(clean); err != nil {
				return err
			}
		} else if config.CloseOldIndexes {
			clean := strings.Join(cleanup, ",")
			if _, err := conn.Flush(clean); err != nil {
				return err
			}
			if _, err := conn.CloseIndex(clean); err != nil {
				return err
			}
		}
	}
	if len(optimizes) > 0 {
		if config.SettingsOnRoll != nil {
			optimize := strings.Join(optimizes, ",")
			if err := PutSettings(conn, optimize, config.SettingsOnRoll); err != nil {
				return err
			}
		}
		if config.OptimizeOnRoll {
			args := make(map[string]interface{})
			if config.OptimizeMaxSegments != 0 {
				args["max_num_segments"] = config.OptimizeMaxSegments
			}
			if _, err := conn.OptimizeIndices(args, optimizes...); err != nil {
				return err
			}
		}
	}
	return nil
}

func (config *IndexConfig) RollsOnSize() bool {
	return config.RollUnit == "bytes"
}

func (config *IndexConfig) ShouldRoll(t time.Time) bool {
	var roll bool = false
	if !config.RollsOnSize() {
		if config.RollUnit == "minutes" {
			roll = t.Second() == 0
			if config.RollIncrement != 0 {
				roll = roll && t.Minute()%config.RollIncrement == 0
			}
		} else if config.RollUnit == "hours" {
			roll = t.Second() == 0 && t.Minute() == 0
			if config.RollIncrement != 0 {
				roll = roll && t.Hour()%config.RollIncrement == 0
			}
		} else {
			roll = t.Second() == 0 && t.Minute() == 0 && t.Hour() == 0
			if config.RollIncrement != 0 {
				if config.RollUnit == "days" {
					roll = roll && t.YearDay()%config.RollIncrement == 0
				} else if config.RollUnit == "months" {
					roll = roll && int(t.Month())%config.RollIncrement == 0
				} else if config.RollUnit == "years" {
					roll = roll && t.Year()%config.RollIncrement == 0
				}
			}
		}
	}
	return roll
}

func (config *IndexConfig) OldIndexNames(conn *elastigo.Conn) []Index {
	var indexes []Index
	pattern := config.TargetIndex + "_*"
	catIndexInfo := conn.GetCatIndexInfo(pattern)
	for _, info := range catIndexInfo {
		indexes = append(indexes, Index{Name: info.Name, Status: info.Status})
	}
	sort.Sort(ByIndexAge(indexes))
	return indexes
}

func AddWork(t time.Time) bool {
	return t.Second() == 0
}

func (config *EsRollConfig) ConfigTransport() error {
	if config.ElasticPemFile != "" {
		certs := x509.NewCertPool()
		if ca, err := ioutil.ReadFile(config.ElasticPemFile); err == nil {
			certs.AppendCertsFromPEM(ca)
		} else {
			return err
		}
		tlsConfig := &tls.Config{RootCAs: certs}
		http.DefaultTransport.(*http.Transport).TLSClientConfig = tlsConfig
	}
	return nil
}

func main() {
	var mainConfig EsRollConfig
	flag.StringVar(&mainConfig.ElasticUrl, "url", "", "ElasticSearch connection URL")
	flag.StringVar(&mainConfig.ElasticPemFile, "pem", "", "Path to a PEM file for secure connections to ElasticSearch")
	flag.BoolVar(&mainConfig.Daemon, "daemon", false, "Run as a daemon")
	flag.Parse()
	if err := mainConfig.ConfigTransport(); err != nil {
		panic(err)
	}
	conn := elastigo.NewConn()
	if mainConfig.ElasticUrl != "" {
		conn.SetFromUrl(mainConfig.ElasticUrl)
	}
	configs, err := GetConfigs(conn)
	if err != nil {
		if mainConfig.Daemon {
			fmt.Println("Configuration for esroll invalid or not found, waiting till one exists")
		} else {
			fmt.Println("Configuration for esroll invalid or not found")
		}
		fmt.Println("You can create one with ...")
		fmt.Println(ExampleConfig)
		if !mainConfig.Daemon {
			os.Exit(1)
		}
	}
	if mainConfig.Daemon {
		var configTicker = time.NewTicker(10 * time.Second)
		var sizeTicker = time.NewTicker(10 * time.Second)
		var workQ = make(chan (time.Time))
		var initQ = make(chan (IndexConfig))
		go func(conn *elastigo.Conn) {
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
			<-sigs
			configTicker.Stop()
			sizeTicker.Stop()
			conn.Close()
			os.Exit(0)
		}(conn)
		go func() {
			var clock = time.NewTicker(1 * time.Second)
			for {
				t := <-clock.C
				t = t.UTC()
				if AddWork(t) {
					workQ <- t
				}
			}
		}()
		for {
			select {
			case t := <-workQ:
				for _, conf := range configs {
					if conf.ShouldRoll(t) {
						if err := conf.Roll(conn, t); err != nil {
							fmt.Println(err)
						}
					}
				}
			case <-sizeTicker.C:
				for _, conf := range configs {
					if conf.RollsOnSize() {
						if err := conf.Roll(conn, time.Now().UTC()); err != nil {
							fmt.Println(err)
						}
					}
				}
			case conf := <-initQ:
				if err := conf.Roll(conn, time.Now().UTC()); err != nil {
					fmt.Println(err)
				}
			case <-configTicker.C:
				configs, err = GetConfigs(conn)
				if err == nil {
					for _, config := range configs {
						exists, _ := conn.ExistsIndex(config.TargetIndex, "", nil)
						if !exists {
							go func() {
								initQ <- config
							}()
						}
					}
				} else {
					fmt.Println(err)
				}
			}
		}
	} else {
		for _, conf := range configs {
			if err := conf.Roll(conn, time.Now().UTC()); err != nil {
				fmt.Println(err)
			}
		}
	}
}
