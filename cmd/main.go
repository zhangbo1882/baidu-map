package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	_ "io/ioutil"
	_ "net/http"
	"net/url"
	"os"
	"sort"
	_ "strconv"
	_ "strings"
	"sync"
	"time"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	"github.com/go-resty/resty/v2"
	"github.com/qiniu/qmgo"
	"github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	max_workers                          = 40
	MIN                                  = 0.00000001
	nearest_offices                      = 10
	mongo_url                            = "mongodb://10.249.64.55:27017"
	mongo_database                       = "local"
	mongo_collection                     = "pingan"
	myak                                 = "w5i9dYBqFBNR3ukdvsfpuEe40Cr53OSl"
	sk                                   = "TGXfG0jcHTegDV0aSpQXRMtApCANqtOe"
	place                                = "/place/v2/search?query=%s&region=%s&output=json&ak=" + myak
	path_transport                       = "/directionlite/v1/transit?origin=%s,%s&destination=%s,%s&timestamp=%s&ak=" + myak
	path_walk                            = "/directionlite/v1/walking?origin=%s,%s&destination=%s,%s&timestamp=%s&ak=" + myak
	path_drive                           = "/directionlite/v1/driving?origin=%s,%s&destination=%s,%s&timestamp=%s&ak=" + myak
	path_ride                            = "/directionlite/v1/riding?origin=%s,%s&destination=%s,%s&timestamp=%s&ak=" + myak
	office_number_max                    = 100
	person_number_max                    = 200
	default_region                       = "上海"
	host                                 = "http://api.map.baidu.com"
	execel_file                          = "data.xlsx"
	sheet_person                         = "persons"
	sheet_office                         = "offices"
	sheet_person_name_index              = 0
	sheet_person_address_index           = 1
	sheet_person_poi_index               = 2
	sheet_person_path_index              = 3
	sheet_person_office1_index           = 4
	sheet_person_office1_duration1_index = 5
	sheet_person_office2_index           = 6
	sheet_person_office2_duration2_index = 7
	sheet_person_office3_index           = 8
	sheet_person_office3_duration3_index = 9
	sheet_person_poi_name                = "C"
	sheet_person_office1_name            = "E"
	sheet_person_office1_duration1_name  = "F"
	sheet_person_office2_name            = "G"
	sheet_person_office2_duration2_name  = "H"
	sheet_person_office3_name            = "I"
	sheet_person_office3_duration3_name  = "J"
	sheet_office_name_index              = 0
	sheet_office_address_index           = 1
	sheet_office_poi_index               = 2
	sheet_office_poi_name                = "C"
)

var (
	path_type    = []string{"walk", "ride", "transport", "drive"}
	path_map     = map[string]string{"walk": path_walk, "ride": path_ride, "transport": path_transport, "drive": path_drive}
	time_format  = "2006-01-02 15:04:05"
	time_morning = "2021-10-11 07:00:00"
)

type Map struct {
	restyClient   *resty.Client
	ctx           context.Context
	mongoCli      *qmgo.QmgoClient
	log           *logrus.Logger
	excelFile     *excelize.File
	personSlice   []Person
	officeSlice   []Office
	currentWorker int
	lock          *sync.Mutex
	wg            sync.WaitGroup
}

type Poi struct {
	Lat float64 `json:"lat" bson:"lat"`
	Lng float64 `json:"lng" bson:"lng"`
}

type Duration struct {
	Sort         []string       // sort according to the duration [drive, transport, ride, walk]
	DurationPath map[string]int `bson:"duration_path"` // key: walk/drive/ride/transport
}

type Person struct {
	Id               primitive.ObjectID        `bson:"_id,omitempty"`
	Name             string                    `bson:"name"`
	Address          string                    `bson:"address"`
	ExcelCell        string                    `bson:"excel_cell,omitempty"`
	CanDrive         bool                      `bson:"can_drive"`
	Poi              Poi                       `bson:"poi,omitempty"`
	DurationMap      map[string]Duration       `bson:"duration_map,omitempty"`    // key: office.name
	NearestOffices   [10]string                `bson:"nearest_offices,omitempty"` // save 10 nearest offices
	NearestDurations [10]int                   `bson:"nearest_durations,omitempty"`
	SortList         []int                     `bson:"sort_list,omitempty"` // ordered duration value
	SortMap          map[int][]DesignateOffice `bson:"sort_map,omitempty"`  // get office by the ordered duration value
	Done             bool                      `bson:"done,omitempty"`
}

type Dummy struct {
	PersonName string `bson:"person_name"`
	Path       string `bson:"path"`
	Duration   int    `bson:"duration"`
}

type Office struct {
	Id        primitive.ObjectID `bson:"_id,omitempty"`
	Name      string             `bson:"name"`
	Address   string             `bson:"address"`
	ExcelCell string             `bson:"excel_cell,omitempty"`
	Poi       Poi                `bson:"poi,omitempty"`
	SortMap   map[int][]Dummy    `bson:"sort_map,omitempty"`
	SortList  []Dummy            `bson:"sort_list,omitempty"`
}

type DesignateOffice struct {
	Path string
	Name string
}

type PlaceRespResult struct {
	Name     string `json:"name"`
	Location Poi    `json:"location"`
}

type PlaceResp struct {
	Status  int               `json:"status"`
	Message string            `json:"message"`
	Results []PlaceRespResult `json:"results"`
}

type Route struct {
	Distance int `json:"distance"`
	Duration int `json:"duration"`
}
type PathPlanRespResult struct {
	Routes []Route `json:"routes"`
}

type PathPlan struct {
	Status  int                `json:"status"`
	Message string             `json:"message"`
	Result  PathPlanRespResult `json:"result"`
}

func init() {
	formatter := runtime.Formatter{ChildFormatter: &logrus.TextFormatter{
		FullTimestamp: true,
	}}
	logrus.SetFormatter(&formatter)
	logrus.SetLevel(logrus.InfoLevel)
	logrus.WithFields(logrus.Fields{
		"file": "main.go",
	})
}

func generateSN(path string) string {
	rawStr := url.QueryEscape(path + sk)
	hasher := md5.New()
	hasher.Write([]byte(rawStr))
	hexStr := hex.EncodeToString(hasher.Sum(nil))
	return hexStr
}

func (m *Map) loadExecelData(file string) error {
	f, err := excelize.OpenFile(file)
	if err != nil {
		m.log.Errorf("Can not load excel file, err: %v", err)
		os.Exit(1)
	}
	rows, err := f.GetRows(sheet_person)
	if err != nil {
		m.log.Errorf("Can not get rows, err: %v", err)
		os.Exit(1)
	}
	for index, row := range rows {
		if index == 0 {
			continue
		}
		if len(row) < sheet_person_address_index+1 {
			continue
		}
		name := row[sheet_person_name_index]
		p := Person{
			DurationMap: make(map[string]Duration, office_number_max),
			SortMap:     make(map[int][]DesignateOffice, office_number_max),
			Done:        false,
		}
		err := m.mongoCli.Find(m.ctx, bson.M{"name": name}).One(&p)
		if err != nil {
			m.log.Infof("%v does not exist, err: %v", name, err)
			p.Name = name
			p.Address = row[sheet_person_address_index]
			if row[sheet_person_path_index] == "Yes" {
				p.CanDrive = true
			} else {
				p.CanDrive = false
			}
			_, err = m.mongoCli.InsertOne(m.ctx, p)
			m.log.Infof("create new one, err: %v", err)
		} else {
			if row[sheet_person_path_index] == "Yes" {
				p.CanDrive = true
			} else {
				p.CanDrive = false
			}
			_, err = m.mongoCli.UpsertId(m.ctx, p.Id, p)
			if err != nil {
				m.log.Infof("%v updating fails, err: %v", name, err)
			}
		}
		p.SortList = []int{}
		p.SortMap = make(map[int][]DesignateOffice, office_number_max)
		p.NearestOffices = [10]string{}
		p.NearestDurations = [10]int{}
		m.personSlice = append(m.personSlice, p)
		m.log.Debugf("Person: %v, %v", p.Name, p.Address)
	}

	rows, err = f.GetRows(sheet_office)
	if err != nil {
		m.log.Errorf("Can not get rows, err: %v", err)
		os.Exit(1)
	}
	for index, row := range rows {
		if index == 0 {
			continue
		}
		if len(row) < sheet_office_address_index+1 {
			continue
		}
		name := row[sheet_office_name_index]
		o := Office{
			SortMap: make(map[int][]Dummy, person_number_max),
		}
		err := m.mongoCli.Find(m.ctx, bson.M{"name": name}).One(&o)
		if err != nil {
			o.Name = name
			o.Address = row[sheet_office_name_index]
			_, err = m.mongoCli.InsertOne(m.ctx, o)
			m.log.Infof("%v does not exist, create new one, err: %v", name, err)
		}
		o.SortMap = make(map[int][]Dummy, person_number_max)
		o.SortList = []Dummy{}
		m.officeSlice = append(m.officeSlice, o)
		m.log.Debugf("Office: %v, %v", o.Name, o.Address)
	}
	m.excelFile = f
	return nil

}

func (m *Map) getPoi(addr string) (Poi, error) {
	path := fmt.Sprintf(place, url.QueryEscape(addr), url.QueryEscape(default_region))
	sn := generateSN(path)
	m.log.Debugf("path: %v sn: %s", path, sn)
	urlPath := fmt.Sprintf(host+place+"&sn=%s", url.QueryEscape(addr), url.QueryEscape(default_region), sn)
	resp, err := m.restyClient.R().EnableTrace().Get(urlPath)
	if err != nil {
		m.log.Errorf("Get %v fails", urlPath)
		return Poi{}, err
	}
	m.log.Debugf("Resp: %v", string(resp.Body()))
	var placeResp PlaceResp
	err = json.Unmarshal(resp.Body(), &placeResp)
	if err != nil {
		m.log.Errorf("Parse resp data fails, err: %v", err)
		return Poi{}, err
	}
	m.log.Debugf("resp: %+v", placeResp)
	if placeResp.Status != 0 || len(placeResp.Results) < 1 {
		m.log.Errorf("Can not get poi from server, message: %v", placeResp.Message)
		return Poi{}, fmt.Errorf("Can not get poi from server, message: %v", placeResp.Message)
	}
	return placeResp.Results[0].Location, nil
}
func IsEqual(f1, f2 float64) bool {
	if f1 > f2 {
		return f1-f2 < MIN
	} else {
		return f2-f1 < MIN
	}
}

func (m *Map) getAllPoi() {
	//defer m.excelFile.Save()
	for index, person := range m.personSlice {
		poi := m.personSlice[index].Poi
		if IsEqual(poi.Lat, 0) && IsEqual(poi.Lng, 0) {
			poi, err := m.getPoi(m.personSlice[index].Address)
			if err != nil {
				m.log.Errorf("Getting poi for %v fails, err: %v", person.Name, err)
			}
			m.personSlice[index].Poi = poi
			//	m.excelFile.SetCellStr(sheet_person, sheet_person_poi_name+strconv.Itoa(index+2), fmt.Sprintf("%f", poi.Lat)+","+fmt.Sprintf("%f", poi.Lng))
			_, err = m.mongoCli.UpsertId(m.ctx, person.Id, m.personSlice[index])
			if err != nil {
				m.log.Errorf("MongoDB updating fails for %v, err: %v", person.Name, err)
			}
		} else {
			m.log.Infof("Poi(%+v) exists for %v", m.personSlice[index].Poi, m.personSlice[index].Name)
		}
	}
	for index, office := range m.officeSlice {
		poi := m.officeSlice[index].Poi
		if IsEqual(poi.Lat, 0) && IsEqual(poi.Lng, 0) {
			poi, err := m.getPoi(m.officeSlice[index].Address)
			if err != nil {
				m.log.Errorf("Getting poi for %v fails, err: %v", office.Name, err)
			}
			m.officeSlice[index].Poi = poi
			//	m.excelFile.SetCellStr(sheet_office, sheet_office_poi_name+strconv.Itoa(index+2), fmt.Sprintf("%f", poi.Lat)+","+fmt.Sprintf("%f", poi.Lng))
			_, err = m.mongoCli.UpsertId(m.ctx, office.Id, m.officeSlice[index])
			if err != nil {
				m.log.Errorf("MongoDB updating fails for %v, err: %v", office.Name, err)
			}
		} else {
			m.log.Infof("Poi(%+v) exists for %v", m.officeSlice[index].Poi, m.officeSlice[index].Name)
		}
	}
}

func (m *Map) calDuration(origin, dest Poi) map[string]int {
	r := make(map[string]int, len(path_type))
	times, err := time.Parse(time_format, time_morning)
	if err != nil {
		m.log.Errorf("Can not convert time: %v, err: %v", time_morning, err)
		return nil
	}
	timestamp := fmt.Sprintf("%d", times.Unix())
	for _, path := range path_type {
		r[path] = 0xffffffff
		pathStr := fmt.Sprintf(path_map[path], fmt.Sprintf("%f", origin.Lat), fmt.Sprintf("%f", origin.Lng), fmt.Sprintf("%f", dest.Lat), fmt.Sprintf("%f", dest.Lng), timestamp)
		m.log.Debug(pathStr)
		sn := generateSN(pathStr)
		urlPath := fmt.Sprintf(host+pathStr+"&sn=%s", sn)
		resp, err := m.restyClient.R().EnableTrace().Get(urlPath)
		if err != nil {
			m.log.Errorf("Get %v fails", urlPath)
			continue
		}
		var pathPlan PathPlan
		err = json.Unmarshal(resp.Body(), &pathPlan)
		if err != nil {
			m.log.Errorf("Parse resp data fails, err: %v", err)
			continue
		}
		if pathPlan.Status != 0 {
			m.log.Errorf("Can not get path plan (%v) from server, status: %v, message: %v", path, pathPlan.Status, pathPlan.Message)
			continue
		}
		r[path] = pathPlan.Result.Routes[0].Duration / 60
	}
	return r
}

func (m *Map) calDurationForAllOffices(person *Person) {
	for _, office := range m.officeSlice {
		m.log.Infof("person : %v, office: %v, done: %v", person.Name, office.Name, person.Done)
		duration := m.calDuration(person.Poi, office.Poi)
		if duration == nil {
			continue
		}
		d := Duration{
			DurationPath: duration,
		}
		d.Sort = func(d map[string]int, canDrive bool) []string {
			values := []int{}
			for k, v := range d {
				if !canDrive && k == "drive" {
					continue
				}
				values = append(values, v)
			}
			sort.Ints(values)
			r := []string{}
			for _, v := range values {
				for path, duration := range d {
					if !canDrive && path == "drive" {
						continue
					}
					if v == duration {
						r = append(r, path)
					}
				}
			}
			return r
		}(d.DurationPath, person.CanDrive)
		if !person.CanDrive {
			d.Sort = append(d.Sort, "drive")
		}
		person.DurationMap[office.Name] = d
	}
	_, err := m.mongoCli.UpsertId(m.ctx, person.Id, person)
	if err != nil {
		m.log.Errorf("MongoDB updating fails for %v, err: %v", person.Name, err)
	}
	person.Done = true
	m.lock.Lock()
	m.currentWorker--
	m.wg.Done()
	m.lock.Unlock()

	/*
		m.excelFile.SetCellStr(sheet_person, sheet_person_office1_name+strconv.Itoa(index+2), m.personSlice[index].NearestOffices[0])
		m.excelFile.SetCellStr(sheet_person, sheet_person_office2_name+strconv.Itoa(index+2), m.personSlice[index].NearestOffices[1])
		m.excelFile.SetCellStr(sheet_person, sheet_person_office3_name+strconv.Itoa(index+2), m.personSlice[index].NearestOffices[2])
		m.excelFile.SetCellInt(sheet_person, sheet_person_office1_duration1_name+strconv.Itoa(index+2), m.personSlice[index].NearestDurations[0])
		m.excelFile.SetCellInt(sheet_person, sheet_person_office2_duration2_name+strconv.Itoa(index+2), m.personSlice[index].NearestDurations[1])
		m.excelFile.SetCellInt(sheet_person, sheet_person_office3_duration3_name+strconv.Itoa(index+2), m.personSlice[index].NearestDurations[2])
	*/

}

func (m *Map) getAllDuration() {
	//	defer m.excelFile.Save()
	for index := range m.personSlice {
		if m.personSlice[index].Done {
			m.log.Infof("%v has done", m.personSlice[index].Name)
			continue
		}
		for {
			m.lock.Lock()
			if m.currentWorker < max_workers {
				m.log.Infof("Current Workers: %d", m.currentWorker)
				m.wg.Add(1)
				go m.calDurationForAllOffices(&m.personSlice[index])
				m.currentWorker++
				m.lock.Unlock()
				break
			} else {
				m.lock.Unlock()
				time.Sleep(5 * time.Second)
			}
		}
	}
	m.wg.Wait()
}
func (m *Map) handle() {
	for index := range m.personSlice {
		m.personSlice[index].designate()
		_, err := m.mongoCli.UpsertId(m.ctx, m.personSlice[index].Id, m.personSlice[index])
		if err != nil {
			m.log.Errorf("MongoDB updating fails for %v, err: %v", m.personSlice[index].Name, err)
		}
	}
}

func (p *Person) designate() {
	for k, v := range p.DurationMap {
		if len(v.Sort) == 0 {
			continue
		}
		d := v.DurationPath[v.Sort[0]]
		p.SortMap[d] = append(p.SortMap[d], DesignateOffice{
			Path: v.Sort[0],
			Name: k,
		})
		p.SortList = append(p.SortList, d)
	}
	sort.Ints(p.SortList)
	done := false
	for i := 0; i < nearest_offices; {
		for j := range p.SortMap[p.SortList[i]] {
			p.NearestOffices[i] = p.SortMap[p.SortList[i]][j].Name
			p.NearestDurations[i] = p.SortList[i]
			i++
			if i >= nearest_offices {
				done = true
				break
			}
		}
		if done {
			break
		}
	}
}

func (m *Map) findPersons() {
	for index, office := range m.officeSlice {
		for _, person := range m.personSlice {
			duration := person.DurationMap[office.Name]
			if len(duration.Sort) == 0 {
				continue
			}
			path := duration.Sort[0]
			d := duration.DurationPath[path]
			m.officeSlice[index].SortMap[d] = append(m.officeSlice[index].SortMap[d], Dummy{
				PersonName: person.Name,
				Path:       path,
				Duration:   d,
			})
		}
		keys := []int{}
		for key := range m.officeSlice[index].SortMap {
			keys = append(keys, key)
		}
		sort.Ints(keys)
		m.log.Infof("%v, keys: %v", office.Name, keys)
		for _, key := range keys {
			for i := range m.officeSlice[index].SortMap[key] {
				m.log.Infof("%v, key: %v, i: %v", office.Name, key, i)
				m.officeSlice[index].SortList = append(m.officeSlice[index].SortList, m.officeSlice[index].SortMap[key][i])
			}
		}
		_, err := m.mongoCli.UpsertId(m.ctx, office.Id, m.officeSlice[index])
		if err != nil {
			m.log.Errorf("MongoDB updating fails for %v, err: %v", office.Name, err)
		}

	}
}

func (p *Person) showDesignate() {
	fmt.Printf("Person Name: %v\n", p.Name)
	for i := range p.SortList {
		fmt.Printf("\tThe nearest office: %v\n", p.NearestOffices[i])
		fmt.Printf("\t\tDuration: %v\n", p.NearestDurations[i])
	}
}

func (m *Map) showPerson() {
	m.log.Infof("Show Person Information")
	for _, person := range m.personSlice {
		m.log.Info(person)
	}
}

func (m *Map) showDesignateAll() {
	for _, person := range m.personSlice {
		person.showDesignate()
	}
}

func main() {
	logger := logrus.StandardLogger()
	ctx := context.Background()
	cli, err := qmgo.Open(ctx, &qmgo.Config{Uri: mongo_url, Database: mongo_database, Coll: mongo_collection})
	if err != nil {
		logger.Errorf("Opening mongo cli fails, err: %v", err)
		os.Exit(1)
	}
	defer cli.Close(ctx)
	m := Map{
		restyClient: resty.New(),
		log:         logger,
		ctx:         ctx,
		mongoCli:    cli,
		lock:        &sync.Mutex{},
	}
	logger.Infof("Load data")
	m.loadExecelData(execel_file)
	m.getAllPoi()
	m.getAllDuration()
	m.handle()
	m.findPersons()

	//	m.showDesignateAll()
	//	m.showPerson()
}
