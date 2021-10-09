package model

import (
	"sort"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	Nearest_Offices = 10
	Nearest_Persons = 20
)

type Person struct {
	Id               primitive.ObjectID        `bson:"_id,omitempty"`
	Name             string                    `bson:"name"`
	Address          string                    `bson:"address"`
	CanDrive         bool                      `bson:"can_drive"`
	Poi              Poi                       `bson:"poi,omitempty"`
	DurationMap      map[string]Duration       `bson:"duration_map,omitempty"`    // key: office.name
	NearestOffices   [Nearest_Offices]string   `bson:"nearest_offices,omitempty"` // save 10 nearest offices
	NearestDurations [Nearest_Offices]int      `bson:"nearest_durations,omitempty"`
	SortList         []int                     `bson:"sort_list,omitempty"` // ordered duration value
	SortMap          map[int][]DesignateOffice `bson:"sort_map,omitempty"`  // get office by the ordered duration value
	Done             bool                      `bson:"done,omitempty"`
}

type Poi struct {
	Lat float64 `json:"lat" bson:"lat"`
	Lng float64 `json:"lng" bson:"lng"`
}

type Duration struct {
	Sort         []string       // sort according to the duration [drive, transport, ride, walk]
	DurationPath map[string]int `bson:"duration_path"` // key: walk/drive/ride/transport
}

type Dummy struct {
	PersonName string `bson:"person_name"`
	Path       string `bson:"path"`
	Duration   int    `bson:"duration"`
}

type Office struct {
	Id       primitive.ObjectID `bson:"_id,omitempty"`
	Name     string             `bson:"name"`
	Address  string             `bson:"address"`
	Poi      Poi                `bson:"poi,omitempty"`
	SortMap  map[int][]Dummy    `bson:"sort_map,omitempty"`
	SortList []Dummy            `bson:"sort_list,omitempty"`
}

type DesignateOffice struct {
	Path string
	Name string
}

func (p *Person) Designate() {
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
	for i := 0; i < Nearest_Offices; {
		for j := range p.SortMap[p.SortList[i]] {
			p.NearestOffices[i] = p.SortMap[p.SortList[i]][j].Name
			p.NearestDurations[i] = p.SortList[i]
			i++
			if i >= Nearest_Offices {
				done = true
				break
			}
		}
		if done {
			break
		}
	}
}
