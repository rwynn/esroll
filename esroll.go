package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/dustin/go-humanize"
	"github.com/olivere/elastic"
	aws "github.com/olivere/elastic/aws/v4"
	"io/ioutil"
	"log"
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

const Version string = "1.3.0"

const ExampleConfig string = `curl -XPUT -H'Content-Type:application/json' localhost:9200/esroll/config/snowball -d '{
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
	ElasticUrl      string
	ElasticUser     string
	ElasticPassword string
	ElasticPemFile  string
	Insecure        bool
	AWSAccessKey    string
	AWSSecretKey    string
	AWSRegion       string
	Daemon          bool
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

func GetConfigs(client *elastic.Client) (Configs, error) {
	search := client.Search("esroll")
	res, err := search.Do(context.Background())
	if err != nil {
		return nil, err
	}
	if res.Hits.TotalHits == 0 {
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
			log.Println(err)
		} else {
			configs = append(configs, configData)
		}
	}
	return configs, nil
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

func (config *IndexConfig) IndexSize(client *elastic.Client) (int64, error) {
	cat := client.CatIndices()
	cat.Index(config.TargetIndex)
	cat.Bytes("b")
	res, err := cat.Do(context.Background())
	if err != nil {
		return 0, err
	}
	infoSize := len(res)
	if infoSize == 0 {
		return 0, errors.New("Unable to find index in size calculation")
	} else if infoSize > 1 {
		return 0, errors.New("Too many indices returned in size calculation")
	} else {
		index := res[0]
		b, err := humanize.ParseBytes(index.PriStoreSize)
		if err != nil {
			return 0, err
		}
		return int64(b), nil
	}
}

func (config *IndexConfig) HasRoom(client *elastic.Client) (bool, error) {
	exists := client.IndexExists(config.TargetIndex)
	ok, _ := exists.Do(context.Background())
	if !ok {
		return false, nil
	} else {
		priSize, err := config.IndexSize(client)
		if err != nil {
			return true, err
		} else {
			maxBytes, err := humanize.ParseBytes(config.RollSize)
			if err != nil {
				return true, err
			} else {
				hasRoom := priSize < int64(maxBytes)
				return hasRoom, nil
			}
		}
	}
}

func (config *IndexConfig) Roll(client *elastic.Client, t time.Time) error {
	nextIndex := config.NextIndex(t)
	if config.RollsOnSize() {
		room, err := config.HasRoom(client)
		if err != nil {
			return err
		} else if room {
			return nil
		}
	} else {
		exists := client.IndexExists(nextIndex)
		ok, _ := exists.Do(context.Background())
		if ok {
			return nil
		}
	}
	settings := make(map[string]interface{})
	if config.Settings != nil {
		settings["settings"] = config.Settings
	}
	oldIndexes := config.OldIndexNames(client)
	createIndex := client.CreateIndex(nextIndex)
	createIndex.BodyJson(settings)
	if _, err := createIndex.Do(context.Background()); err != nil {
		return err
	}
	alias := client.Alias()
	var cleanup, optimizes []string
	var searchSuffix string = "_" + config.SearchSuffix
	alias.Add(nextIndex, config.TargetIndex)
	alias.Add(nextIndex, config.TargetIndex+searchSuffix)
	searchIndexes := 1 + len(oldIndexes)
	for i, oldIndex := range oldIndexes {
		alias.Remove(oldIndex.Name, config.TargetIndex)
		retire := (searchIndexes - 1) >= config.IndexesToAliasForSearch
		if retire {
			alias.Remove(oldIndex.Name, config.TargetIndex+searchSuffix)
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
	if _, err := alias.Do(context.Background()); err != nil {
		return err
	}
	if len(cleanup) > 0 {
		if config.DeleteOldIndexes {
			del := client.DeleteIndex(cleanup...)
			if _, err := del.Do(context.Background()); err != nil {
				return err
			}
		} else if config.CloseOldIndexes {
			flush := client.Flush(cleanup...)
			if _, err := flush.Do(context.Background()); err != nil {
				return err
			}
			cls := client.CloseIndex(strings.Join(cleanup, ","))
			if _, err := cls.Do(context.Background()); err != nil {
				return err
			}
		}
	}
	if len(optimizes) > 0 {
		if config.SettingsOnRoll != nil {
			settings := client.IndexPutSettings(optimizes...)
			settings.BodyJson(config.SettingsOnRoll)
			if _, err := settings.Do(context.Background()); err != nil {
				return err
			}
		}
		if config.OptimizeOnRoll {
			merge := client.Forcemerge(optimizes...)
			if config.OptimizeMaxSegments != 0 {
				merge.MaxNumSegments(config.OptimizeMaxSegments)
			}
			if _, err := merge.Do(context.Background()); err != nil {
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
	if config.RollsOnSize() {
		return false
	} else if config.RollUnit == "minutes" {
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
	return roll
}

func (config *IndexConfig) OldIndexNames(client *elastic.Client) []Index {
	cat := client.CatIndices()
	cat.Index(config.TargetIndex + "_*")
	cat.Columns("index", "status")
	res, err := cat.Do(context.Background())
	var indexes []Index
	if err == nil {
		for _, i := range res {
			indexes = append(indexes, Index{Name: i.Index, Status: i.Status})
		}
		sort.Sort(ByIndexAge(indexes))
	}
	return indexes
}

func AddWork(t time.Time) bool {
	return t.Second() == 0
}

func (config *EsRollConfig) NewHTTPClient() (client *http.Client, err error) {
	tlsConfig := &tls.Config{}
	if config.ElasticPemFile != "" {
		var ca []byte
		certs := x509.NewCertPool()
		if ca, err = ioutil.ReadFile(config.ElasticPemFile); err == nil {
			certs.AppendCertsFromPEM(ca)
			tlsConfig.RootCAs = certs
		} else {
			return client, err
		}
	}
	if config.Insecure {
		// Turn off validation
		tlsConfig.InsecureSkipVerify = true
	}
	transport := &http.Transport{
		TLSHandshakeTimeout: time.Duration(30) * time.Second,
		TLSClientConfig:     tlsConfig,
	}
	client = &http.Client{
		Transport: transport,
	}
	if config.AWSAccessKey != "" {
		client = aws.NewV4SigningClientWithHTTPClient(credentials.NewStaticCredentials(
			config.AWSAccessKey,
			config.AWSSecretKey,
			"",
		), config.AWSRegion, client)
	}
	return client, err
}

func (config *EsRollConfig) needsSecureScheme() bool {
	if config.ElasticUrl != "" {
		if strings.HasPrefix(config.ElasticUrl, "https") {
			return true
		}
	}
	return false
}

func (config *EsRollConfig) newElasticClient() (client *elastic.Client, err error) {
	var clientOptions []elastic.ClientOptionFunc
	var httpClient *http.Client
	if config.needsSecureScheme() {
		clientOptions = append(clientOptions, elastic.SetScheme("https"))
	}
	if config.ElasticUrl != "" {
		clientOptions = append(clientOptions, elastic.SetURL(config.ElasticUrl))
	}
	if config.ElasticUser != "" {
		clientOptions = append(clientOptions, elastic.SetBasicAuth(config.ElasticUser, config.ElasticPassword))
	}
	httpClient, err = config.NewHTTPClient()
	if err != nil {
		return client, err
	}
	clientOptions = append(clientOptions, elastic.SetHttpClient(httpClient))
	return elastic.NewClient(clientOptions...)
}

func main() {
	log.SetPrefix("ERROR ")
	var mainConfig EsRollConfig
	var showVersion bool
	flag.BoolVar(&showVersion, "v", false, "True to print the version number")
	flag.StringVar(&mainConfig.ElasticUrl, "url", "", "ElasticSearch connection URL")
	flag.StringVar(&mainConfig.ElasticUser, "user", "", "ElasticSearch user name")
	flag.StringVar(&mainConfig.ElasticPassword, "pass", "", "ElasticSearch user password")
	flag.StringVar(&mainConfig.ElasticPemFile, "pem", "", "Path to a PEM file for secure connections to ElasticSearch")
	flag.BoolVar(&mainConfig.Daemon, "daemon", false, "Run as a daemon")
	flag.BoolVar(&mainConfig.Insecure, "insecure", false, "Disable TLS validation")
	flag.StringVar(&mainConfig.AWSAccessKey, "aws-access-key", "", "AWS access key")
	flag.StringVar(&mainConfig.AWSSecretKey, "aws-secret-key", "", "AWS secret key")
	flag.StringVar(&mainConfig.AWSRegion, "aws-region", "", "AWS region")
	flag.Parse()
	if showVersion {
		fmt.Println(Version)
		os.Exit(0)
	}
	client, err := mainConfig.newElasticClient()
	if err != nil {
		panic(fmt.Sprintf("Unable to create elasticsearch client: %s", err))
	}
	configs, err := GetConfigs(client)
	if err != nil {
		if mainConfig.Daemon {
			log.Println("Configuration for esroll invalid or not found, waiting till one exists")
		} else {
			log.Println("Configuration for esroll invalid or not found")
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
		go func(client *elastic.Client) {
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
			<-sigs
			configTicker.Stop()
			sizeTicker.Stop()
			client.Stop()
			os.Exit(0)
		}(client)
		go func() {
			var clock = time.NewTicker(1 * time.Second)
			for t := range clock.C {
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
						if err := conf.Roll(client, t); err != nil {
							log.Println(err)
						}
					}
				}
			case <-sizeTicker.C:
				for _, conf := range configs {
					if conf.RollsOnSize() {
						if err := conf.Roll(client, time.Now().UTC()); err != nil {
							log.Println(err)
						}
					}
				}
			case conf := <-initQ:
				if err := conf.Roll(client, time.Now().UTC()); err != nil {
					log.Println(err)
				}
			case <-configTicker.C:
				configs, err = GetConfigs(client)
				if err == nil {
					for _, config := range configs {
						exists := client.IndexExists(config.TargetIndex)
						ok, _ := exists.Do(context.Background())
						if !ok {
							go func(c IndexConfig) {
								initQ <- c
							}(config)
						}
					}
				} else {
					log.Println(err)
				}
			}
		}
	} else {
		for _, conf := range configs {
			if err := conf.Roll(client, time.Now().UTC()); err != nil {
				log.Println(err)
			}
		}
	}
}
