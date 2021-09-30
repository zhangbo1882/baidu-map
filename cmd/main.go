package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	_ "io/ioutil"
	_ "net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
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
	sheet_person_poi_index     = 2
	sheet_person_path_index    = 3
	sheet_person_poi_name      = "C"
	sheet_office_name_index    = 0
	sheet_office_address_index = 1
	sheet_office_poi_index     = 2
	sheet_office_poi_name      = "C"
)

var (
	path_type        = []string{"walk", "ride", "transport", "drive"}
	path_map         = map[string]string{"walk": path_walk, "ride": path_ride, "transport": path_transport, "drive": path_drive}
	timestamp string = fmt.Sprintf("%d", time.Now().Unix())
)

type Map struct {
	restyClient *resty.Client
	log         *logrus.Logger
	excelFile   *excelize.File
	personSlice []Person
	officeSlice []Office
}

type Poi struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type Duration struct {
	sort     []string       // sort according to the duration [drive, transport, ride, walk]
	duration map[string]int // key: walk/drive/ride/transport
}

type Person struct {
	name        string
	address     string
	excelCell   string
	canDrive    bool
	poi         Poi
	durationMap map[string]Duration     // key: office.name
	sortList    []int                   // ordered duration value
	sortMap     map[int]DesignateOffice // get office by the ordered duration value
}

type Office struct {
	name      string
	address   string
	excelCell string
	poi       Poi
}

type DesignateOffice struct {
	path string
	name string
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
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.InfoLevel)
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
		address := row[sheet_person_address_index]
		canDrive := false
		if strings.EqualFold(row[sheet_person_path_index], "yes") {
			canDrive = true
		}
		m.personSlice = append(m.personSlice, Person{
			name:        name,
			address:     address,
			canDrive:    canDrive,
			durationMap: make(map[string]Duration, office_number_max),
			sortMap:     make(map[int]DesignateOffice, office_number_max),
		})
		m.log.Debugf("Person: %v, %v", name, address)
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
		address := row[sheet_office_address_index]

		m.officeSlice = append(m.officeSlice, Office{
			name:    name,
			address: address,
		})
		m.log.Debugf("Office: %v, %v", name, address)
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
	if placeResp.Status != 0 {
		m.log.Errorf("Can not get poi from server, message: %v", placeResp.Message)
		return Poi{}, fmt.Errorf("Can not get poi from server, message: %v", placeResp.Message)
	}
	return placeResp.Results[0].Location, nil
}

func (m *Map) getAllPoi() {
	for index := range m.personSlice {
		poiValue, _ := m.excelFile.GetCellValue(sheet_person, sheet_person_poi_name+strconv.Itoa(index+2))
		m.log.Debugf("poiValue: %v", poiValue)
		if poiValue == "0,0" {
			poi, err := m.getPoi(m.personSlice[index].address)
			if err != nil {
				m.log.Error(err)
			}
			m.personSlice[index].poi = poi
			m.excelFile.SetCellStr(sheet_person, sheet_person_poi_name+strconv.Itoa(index+2), fmt.Sprintf("%f", poi.Lat)+","+fmt.Sprintf("%f", poi.Lng))
		} else {
			m.log.Infof("Poi exists for %v", m.personSlice[index].name)
			poiSlice := strings.Split(poiValue, ",")
			if len(poiSlice) != 2 {
				m.log.Errorf("Poi value format is incorrect")
			}
			if v, err := strconv.ParseFloat(poiSlice[0], 64); err == nil {
				m.personSlice[index].poi.Lat = v
			}
			if v, err := strconv.ParseFloat(poiSlice[1], 64); err == nil {
				m.personSlice[index].poi.Lng = v
			}
		}
	}
	m.excelFile.Save()
	for index := range m.officeSlice {
		poiValue, _ := m.excelFile.GetCellValue(sheet_office, sheet_office_poi_name+strconv.Itoa(index+2))
		m.log.Debugf("poiValue: %v", poiValue)
		if poiValue == "0,0" {
			poi, err := m.getPoi(m.officeSlice[index].address)
			if err != nil {
				m.log.Error(err)
			}
			m.officeSlice[index].poi = poi
			m.excelFile.SetCellStr(sheet_office, sheet_office_poi_name+strconv.Itoa(index+2), fmt.Sprintf("%f", poi.Lat)+","+fmt.Sprintf("%f", poi.Lng))
		} else {
			m.log.Infof("Poi exists for %v", m.officeSlice[index].name)
			poiSlice := strings.Split(poiValue, ",")
			if len(poiSlice) != 2 {
				m.log.Errorf("Poi value format is incorrect")
			}
			if v, err := strconv.ParseFloat(poiSlice[0], 64); err == nil {
				m.officeSlice[index].poi.Lat = v
			}
			if v, err := strconv.ParseFloat(poiSlice[1], 64); err == nil {
				m.officeSlice[index].poi.Lng = v
			}
		}
	}
	m.excelFile.Save()
}

func (m *Map) calDuration(orig, dest Poi) map[string]int {
	r := make(map[string]int, len(path_type))
	for _, path := range path_type {
		pathStr := fmt.Sprintf(path_map[path], fmt.Sprintf("%f", orig.Lat), fmt.Sprintf("%f", orig.Lng), fmt.Sprintf("%f", dest.Lat), fmt.Sprintf("%f", dest.Lng), timestamp)
		m.log.Debug(pathStr)
		sn := generateSN(pathStr)
		urlPath := fmt.Sprintf(host+pathStr+"&sn=%s", sn)
		resp, err := m.restyClient.R().EnableTrace().Get(urlPath)
		if err != nil {
			m.log.Errorf("Get %v fails", urlPath)
			return nil
		}
		m.log.Debugf("Resp: %v", string(resp.Body()))
		var pathPlan PathPlan
		err = json.Unmarshal(resp.Body(), &pathPlan)
		if err != nil {
			m.log.Errorf("Parse resp data fails, err: %v", err)
			return nil
		}
		m.log.Debugf("resp: %+v", pathPlan)
		if pathPlan.Status != 0 {
			m.log.Errorf("Can not get poi from server, message: %v", pathPlan.Message)
			return nil
		}
		m.log.Debugf("duration: %v", pathPlan.Result.Routes[0].Duration)
		r[path] = pathPlan.Result.Routes[0].Duration / 60
	}
	return r
}

func (m *Map) getAllDuration() {
	for index, person := range m.personSlice {
		for _, office := range m.officeSlice {
			m.log.Debugf("person poi: %v, office poi: %v", person.poi, office.poi)
			d := Duration{
				duration: m.calDuration(person.poi, office.poi),
			}
			d.sort = func(d map[string]int, canDrive bool) []string {
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
			}(d.duration, person.canDrive)
			if !person.canDrive {
				d.sort = append(d.sort, "drive")
			}
			m.personSlice[index].durationMap[office.name] = d
		}
	}
}

func (p *Person) designate() {
	for k, v := range p.durationMap {
		if len(v.sort == 0) {
			continue
		}
		d := v.duration[v.sort[0]]
		p.sortMap[d] = DesignateOffice{
			path: v.sort[0],
			name: k,
		}
		p.sortList = append(p.sortList, d)
	}
	sort.Ints(p.sortList)
}

func (p *Person) showDesignate() {
	fmt.Printf("Person Name: %v\n", p.name)
	for _, duration := range p.sortList {
		fmt.Printf("\tThe nearest office: %v\n", p.sortMap[duration].name)
		fmt.Printf("\t\tDuration: %v (%v)\n", duration, p.sortMap[duration].path)
	}
}

func (m *Map) designateAll() {
	for i := range m.personSlice {
		m.personSlice[i].designate()
	}
}

func (m *Map) showPerson() {
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
	m := Map{
		restyClient: resty.New(),
		log:         logger,
	}
	logger.Infof("Load data")
	m.loadExecelData(execel_file)
	m.getAllPoi()
	m.getAllDuration()
	m.designateAll()
	m.showPerson()
	m.showDesignateAll()
}
