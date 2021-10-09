package db

import (
	"context"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	"github.com/qiniu/qmgo"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	_ "go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/zhangbo1882/baidu-map/pkg/model"
)

const (
	mongo_url        = "mongodb://10.249.64.55:27017"
	mongo_database   = "local"
	mongo_collection = "pingan"
)

type MongoClient struct {
	Ctx      context.Context
	MongoCli *qmgo.QmgoClient
	log      *logrus.Logger
}

func (m *MongoClient) Find(name string, data interface{}) (r interface{}, err error) {
	switch data.(type) {
	case model.Person:
		p := model.Person{Done: false}
		err = m.MongoCli.Find(m.Ctx, bson.M{"name": name}).One(&p)
		return p, err
	case model.Office:
		o := model.Office{}
		err = m.MongoCli.Find(m.Ctx, bson.M{"name": name}).One(&o)
		return o, err
	}
	return nil, nil
}

func (m *MongoClient) Insert(e interface{}) error {
	_, err := m.MongoCli.InsertOne(m.Ctx, e)
	return err
}

func (m *MongoClient) Update(name string, e interface{}) error {
	err := m.MongoCli.UpdateOne(m.Ctx, bson.M{"name": name}, bson.M{"$set": e})
	return err
}

func (m *MongoClient) Delete(name string) error {
	return nil
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

func NewMongoClient() *MongoClient {
	logger := logrus.StandardLogger()
	ctx := context.Background()
	cli, err := qmgo.Open(ctx, &qmgo.Config{Uri: mongo_url, Database: mongo_database, Coll: mongo_collection})
	if err != nil {
		logger.Errorf("Opening mongo cli fails, err: %v", err)
		return nil
	}
	return &MongoClient{
		Ctx:      ctx,
		MongoCli: cli,
		log:      logger,
	}
}
