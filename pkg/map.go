package controller

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	_ "io/ioutil"
	_ "net/http"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	_ "strings"
	"sync"
	"time"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"

	"github.com/zhangbo1882/baidu-map/pkg/db"
	"github.com/zhangbo1882/baidu-map/pkg/model"
)

const (
	max_workers                = 40
	MIN                        = 0.00000001
	myak                       = "w5i9dYBqFBNR3ukdvsfpuEe40Cr53OSl"
	sk                         = "TGXfG0jcHTegDV0aSpQXRMtApCANqtOe"
	place                      = "/place/v2/search?query=%s&region=%s&output=json&ak=" + myak
	path_transport             = "/directionlite/v1/transit?origin=%s,%s&destination=%s,%s&timestamp=%s&ak=" + myak
	path_walk                  = "/directionlite/v1/walking?origin=%s,%s&destination=%s,%s&timestamp=%s&ak=" + myak
	path_drive                 = "/directionlite/v1/driving?origin=%s,%s&destination=%s,%s&timestamp=%s&ak=" + myak
	path_ride                  = "/directionlite/v1/riding?origin=%s,%s&destination=%s,%s&timestamp=%s&ak=" + myak
	office_number_max          = 100
	person_number_max          = 200
	default_region             = "上海"
	host                       = "http://api.map.baidu.com"
	execel_file                = "data.xlsx"
	sheet_person               = "persons"
	sheet_office               = "offices"
	sheet_person_name_index    = 0
	sheet_person_address_index = 1
	sheet_person_path_index    = 2
	sheet_person_result_start  = 'D'
	sheet_office_name_index    = 0
	sheet_office_address_index = 1
	sheet_office_result_start  = 'C'
)

var (
	path_type    = []string{"walk", "ride", "transport", "drive"}
	path_map     = map[string]string{"walk": path_walk, "ride": path_ride, "transport": path_transport, "drive": path_drive}
	time_format  = "2006-01-02 15:04:05" //The format must use this string
	time_morning = "2021-10-11 07:00:00"
)

type DataBase interface {
	Find(name string, data interface{}) (out interface{}, err error)
	Insert(e interface{}) error
	Update(name string, e interface{}) error
	Delete(name string) error
}

type Map struct {
	restyClient   *resty.Client
	log           *logrus.Logger
	excelFile     *excelize.File
	personSlice   []model.Person
	officeSlice   []model.Office
	currentWorker int
	lock          *sync.Mutex
	wg            sync.WaitGroup
	db            DataBase
}

type PlaceRespResult struct {
	Name     string    `json:"name"`
	Location model.Poi `json:"location"`
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

type Config struct {
	LogLevel logrus.Level
}

func init() {
	formatter := runtime.Formatter{
		ChildFormatter: &logrus.TextFormatter{
			FullTimestamp: true,
		},
		Line:         true,
		File:         true,
		BaseNameOnly: true}
	logrus.SetFormatter(&formatter)
	logrus.SetLevel(logrus.InfoLevel)
}

func string2Bool(s string) bool {
	if s == "Yes" || s == "Y" || s == "y" {
		return true
	} else {
		return false
	}
}

func IsEqual(f1, f2 float64) bool {
	if f1 > f2 {
		return f1-f2 < MIN
	} else {
		return f2-f1 < MIN
	}
}

/*Refer to http://lbsyun.baidu.com/apiconsole/key?application=key*/
func generateSN(path string) string {
	rawStr := url.QueryEscape(path + sk)
	hasher := md5.New()
	hasher.Write([]byte(rawStr))
	hexStr := hex.EncodeToString(hasher.Sum(nil))
	return hexStr
}

func (m *Map) LoadExecelData(file string) error {
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
		p := model.Person{
			DurationMap: make(map[string]model.Duration, office_number_max),
			SortMap:     make(map[int][]model.DesignateOffice, office_number_max),
			Done:        false,
		}
		//		err := m.mongoCli.Find(m.ctx, bson.M{"name": name}).One(&p)
		d, err := m.db.Find(name, model.Person{})
		if err != nil {
			m.log.Debugf("%v does not exist, err: %v", name, err)
			p.Name = name
			p.Address = row[sheet_person_address_index]
			p.CanDrive = string2Bool(row[sheet_person_path_index])
			//_, err = m.mongoCli.InsertOne(m.ctx, p)
			err = m.db.Insert(p)
			m.log.Debugf("create new one, err: %v", err)
		} else {

			ok := false
			p, ok = d.(model.Person)
			if !ok {
				m.log.Errorf("Data is not a person (%v), type: %v", name, reflect.TypeOf(d))
				continue
			}
			m.log.Debugf("p: %+v", p)
			changes := false
			if string2Bool(row[sheet_person_path_index]) != p.CanDrive {
				p.CanDrive = string2Bool(row[sheet_person_path_index])
				changes = true

			}
			if row[sheet_person_address_index] != p.Address {
				m.log.Infof("%v's data changes(from %v to %v), reset its result", name, p.Address, row[sheet_person_address_index])
				p.Address = row[sheet_person_address_index]
				p.Poi = model.Poi{Lat: 0, Lng: 0}
				changes = true
			}
			if changes {
				p.Done = false
				//err = m.mongoCli.UpsertId(m.ctx, p.Id, p)
				err = m.db.Update(p.Name, p)
				m.log.Infof("%v's data changes, reset its result", name)
				if err != nil {
					m.log.Errorf("%v updating fails, err: %v", name, err)
				}
			}
		}
		p.SortList = []int{}
		p.SortMap = make(map[int][]model.DesignateOffice, office_number_max)
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
	officeChanged := false
	for index, row := range rows {
		if index == 0 {
			continue
		}
		if len(row) < sheet_office_address_index+1 {
			continue
		}
		name := row[sheet_office_name_index]
		o := model.Office{
			SortMap: make(map[int][]model.Dummy, person_number_max),
		}
		//err := m.mongoCli.Find(m.ctx, bson.M{"name": name}).One(&o)
		d, err := m.db.Find(name, model.Office{})
		if err != nil {
			o.Name = name
			o.Address = row[sheet_office_address_index]
			//_, err = m.mongoCli.InsertOne(m.ctx, o)
			err = m.db.Insert(o)
			m.log.Debugf("%v does not exist, create new one, err: %v", name, err)
			officeChanged = true
		} else {
			ok := false
			o, ok = d.(model.Office)
			if !ok {
				m.log.Errorf("Data is not a office, type: %v", reflect.TypeOf(d))
				continue
			}
			if row[sheet_office_address_index] != o.Address {
				m.log.Infof("%v's data changes(from %v to %v), reset its result", name, o.Address, row[sheet_office_address_index])
				o.Address = row[sheet_office_address_index]
				o.Poi = model.Poi{Lat: 0, Lng: 0}
				//	_, err = m.mongoCli.UpsertId(m.ctx, o.Id, o)
				err = m.db.Update(o.Name, o)
				if err != nil {
					m.log.Errorf("%v updating fails, err: %v", name, err)
				}
				officeChanged = true
			}
		}
		o.SortMap = make(map[int][]model.Dummy, person_number_max)
		o.SortList = []model.Dummy{}
		m.officeSlice = append(m.officeSlice, o)
		m.log.Debugf("Office: %v, %v", o.Name, o.Address)
	}
	if officeChanged {
		m.log.Infof("Office changes, need reset result")
		for i := range m.personSlice {
			m.personSlice[i].Done = false
		}
	}
	m.excelFile = f
	return nil

}

func (m *Map) getPoi(addr string) (model.Poi, error) {
	path := fmt.Sprintf(place, url.QueryEscape(addr), url.QueryEscape(default_region))
	sn := generateSN(path)
	m.log.Debugf("path: %v sn: %s", path, sn)
	urlPath := fmt.Sprintf(host+place+"&sn=%s", url.QueryEscape(addr), url.QueryEscape(default_region), sn)
	resp, err := m.restyClient.R().EnableTrace().Get(urlPath)
	if err != nil {
		m.log.Errorf("Get %v fails", urlPath)
		return model.Poi{}, err
	}
	m.log.Debugf("Resp: %v", string(resp.Body()))
	var placeResp PlaceResp
	err = json.Unmarshal(resp.Body(), &placeResp)
	if err != nil {
		m.log.Errorf("Parse resp data fails, err: %v", err)
		return model.Poi{}, err
	}
	m.log.Debugf("resp: %+v", placeResp)
	if placeResp.Status != 0 || len(placeResp.Results) < 1 {
		m.log.Errorf("Can not get poi from server, message: %v", placeResp.Message)
		return model.Poi{}, fmt.Errorf("Can not get poi from server, message: %v", placeResp.Message)
	}
	return placeResp.Results[0].Location, nil
}

func (m *Map) getAllPoi() {
	m.log.Infof("Get poi for address")
	for index, person := range m.personSlice {
		poi := m.personSlice[index].Poi
		if IsEqual(poi.Lat, 0) && IsEqual(poi.Lng, 0) {
			poi, err := m.getPoi(m.personSlice[index].Address)
			if err != nil {
				m.log.Errorf("Getting poi for %v fails, err: %v", person.Name, err)
			}
			m.personSlice[index].Poi = poi
			//_, err = m.mongoCli.UpsertId(m.ctx, person.Id, m.personSlice[index])
			err = m.db.Update(person.Name, m.personSlice[index])
			if err != nil {
				m.log.Errorf("MongoDB updating fails for %v, err: %v", person.Name, err)
			}
		} else {
			m.log.Debugf("Poi(%+v) exists for %v", m.personSlice[index].Poi, m.personSlice[index].Name)
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
			//_, err = m.mongoCli.UpsertId(m.ctx, office.Id, m.officeSlice[index])
			err = m.db.Update(office.Name, m.officeSlice[index])
			if err != nil {
				m.log.Errorf("MongoDB updating fails for %v, err: %v", office.Name, err)
			}
		} else {
			m.log.Debugf("Poi(%+v) exists for %v", m.officeSlice[index].Poi, m.officeSlice[index].Name)
		}
	}
}

func (m *Map) calDuration(origin, dest model.Poi) map[string]int {
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

func (m *Map) calDurationForAllOffices(person *model.Person) {
	for _, office := range m.officeSlice {
		m.log.Debugf("person : %v, office: %v, done: %v", person.Name, office.Name, person.Done)
		duration := m.calDuration(person.Poi, office.Poi)
		if duration == nil {
			continue
		}
		d := model.Duration{
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
	//_, err := m.mongoCli.UpsertId(m.ctx, person.Id, person)
	err := m.db.Update(person.Name, person)
	if err != nil {
		m.log.Errorf("MongoDB updating fails for %v, err: %v", person.Name, err)
	}
	person.Done = true
	m.lock.Lock()
	m.currentWorker--
	m.wg.Done()
	m.lock.Unlock()
}

func (m *Map) getAllDuration() {
	m.log.Infof("Get duration from person to office")
	for index := range m.personSlice {
		if m.personSlice[index].Done {
			m.log.Debugf("%v has done", m.personSlice[index].Name)
			continue
		}
		for {
			m.lock.Lock()
			if m.currentWorker < max_workers {
				m.log.Debugf("Current Workers: %d", m.currentWorker)
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
func (m *Map) findOffices() {
	m.log.Infof("Calculate duration to find nearest offices for a person")
	for index := range m.personSlice {
		m.personSlice[index].Designate()
		//_, err := m.mongoCli.UpsertId(m.ctx, m.personSlice[index].Id, m.personSlice[index])
		err := m.db.Update(m.personSlice[index].Name, m.personSlice[index])
		if err != nil {
			m.log.Errorf("MongoDB updating fails for %v, err: %v", m.personSlice[index].Name, err)
		}
	}
}

func (m *Map) findPersons() {
	m.log.Infof("Calculate duration to find nearest persons for an office")
	for index, office := range m.officeSlice {
		for _, person := range m.personSlice {
			duration := person.DurationMap[office.Name]
			if len(duration.Sort) == 0 {
				continue
			}
			path := duration.Sort[0]
			d := duration.DurationPath[path]
			m.officeSlice[index].SortMap[d] = append(m.officeSlice[index].SortMap[d], model.Dummy{
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
		for _, key := range keys {
			for i := range m.officeSlice[index].SortMap[key] {
				m.officeSlice[index].SortList = append(m.officeSlice[index].SortList, m.officeSlice[index].SortMap[key][i])
			}
		}
		//_, err := m.mongoCli.UpsertId(m.ctx, office.Id, m.officeSlice[index])
		err := m.db.Update(office.Name, m.officeSlice[index])
		if err != nil {
			m.log.Errorf("MongoDB updating fails for %v, err: %v", office.Name, err)
		}

	}
}

func (m *Map) WriteToExcel() {
	defer m.excelFile.Save()

	m.log.Infof("Write result to excel file")
	for index := range m.personSlice {
		for i := 0; i < model.Nearest_Offices; i++ {
			m.excelFile.SetCellStr(sheet_person, string(sheet_person_result_start+i)+strconv.Itoa(index+2), m.personSlice[index].NearestOffices[i]+" ("+strconv.Itoa(m.personSlice[index].NearestDurations[i])+")")
		}
	}

	for index := range m.officeSlice {
		for i := 0; i < model.Nearest_Persons; i++ {
			m.excelFile.SetCellStr(sheet_office, string(sheet_office_result_start+i)+strconv.Itoa(index+2), m.officeSlice[index].SortList[i].PersonName+" ("+strconv.Itoa(m.officeSlice[index].SortList[i].Duration)+")")
		}
	}
}

func NewMapController() *Map {
	logger := logrus.StandardLogger()
	database := db.NewMongoClient()
	return &Map{
		restyClient: resty.New(),
		lock:        &sync.Mutex{},
		db:          database,
		log:         logger,
	}
}
func (m *Map) Run() {
	m.LoadExecelData(execel_file)
	m.getAllPoi()
	m.getAllDuration()
	m.findOffices()
	m.findPersons()
	m.WriteToExcel()
}
