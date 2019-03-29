package helper

import (
	"errors"
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/IBM/yarnbeat/config"
	"github.com/IBM/yarnbeat/mapreduce"
	"reflect"
	"strings"
)

func SetEventFields(event *beat.Event, item interface{}) error {
	itemType := reflect.TypeOf(item)
	for i := 0; i < itemType.NumField(); i++ {
		itemField := itemType.Field(i)
		eventField, ok := itemField.Tag.Lookup("field")
		if ok {
			itemValue := reflect.ValueOf(item).Field(i)
			var eventValue interface{}
			if strings.HasPrefix(eventField, ",") {
				if itemValue.Kind() != reflect.String {
					return errors.New(fmt.Sprintf("cannot split non-string field [%s]", itemField.Name))
				}
				eventField = strings.TrimLeft(eventField, ",")
				eventValue = strings.Split(itemValue.String(), ",")
			} else {
				eventValue = itemValue.Interface()
			}

			_, err := event.Fields.Put(eventField, eventValue)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func SetMapReduceEventFields(
	event *beat.Event,
	config []config.MapReduceCounterConfig,
	job *mapreduce.Job,
	counterGroups []mapreduce.CounterGroup) error {
	err := SetEventFields(event, *job)
	if err != nil {
		return err
	}

	groupMap := make(map[string][]mapreduce.Counter)
	for i := range counterGroups {
		cg := counterGroups[i]
		groupMap[cg.CounterGroupName] = cg.Counters
	}

	for _, g := range config {
		counters := groupMap[g.GroupName]
		counterMap := make(map[string]mapreduce.Counter)
		for i := range counters {
			counter := counters[i]
			counterMap[counter.Name] = counter
		}
		for k, v := range g.Counters {
			counterName := strings.Replace(k, "$$", ".", -1)
			if counter, ok := counterMap[counterName]; ok {
				fieldName := fmt.Sprint("mapreduce.job.counters.", v)
				_, err = event.Fields.Put(fieldName, counter.TotalCounterValue)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
