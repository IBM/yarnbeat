package helper

import (
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/IBM/yarnbeat/config"
	"github.com/IBM/yarnbeat/mapreduce"
	"testing"
)

func Test_SetEventFields(t *testing.T) {
	type testType struct {
		Field1 int    `field:"field_1"`
		Field2 string `field:"field_2"`
		Field3 string `field:",field_3"`
	}

	testItem := testType{Field1: 42, Field2: "test", Field3: "test1,test2"}
	event := &beat.Event{Fields: common.MapStr{}}
	err := SetEventFields(event, testItem)
	if err != nil {
		t.Error("Failed to set event fields: ", err.Error())
	}

	field1Value, err := event.Fields.GetValue("field_1")
	if err != nil {
		t.Error("Failed to get value for field_1")
	}
	if field1Value.(int) != testItem.Field1 {
		t.Errorf("Expected field_1 to contain %d, got %d", testItem.Field1, field1Value.(int))
	}

	field2Value, err := event.Fields.GetValue("field_2")
	if err != nil {
		t.Error("Failed to get value for field_2")
	}
	if field2Value.(string) != testItem.Field2 {
		t.Errorf("Expected field_2 to contain %s, got %s", testItem.Field2, field2Value.(string))
	}

	field3Value, err := event.Fields.GetValue("field_3")
	if err != nil {
		t.Error("Failed to get value for field_3")
	}
	if len(field3Value.([]string)) != 2 {
		t.Errorf("Expected field_3 to contain []string of len %d, got %d", 2, len(field3Value.([]string)))
	}
}

func Test_SetMapReduceEventFields(t *testing.T) {
	event := &beat.Event{Fields: common.MapStr{}}
	counterConfig := []config.MapReduceCounterConfig{
		{GroupName: "org.test.group.a", Counters: map[string]string{"counter1": "a.counter_1", "counter2": "a.counter_2"}},
		{GroupName: "org.test.group.b", Counters: map[string]string{"counter1": "b.counter_1", "counter2": "b.counter_2"}},
	}
	job := mapreduce.Job{
		Id:   "job_1234",
		Name: "test job",
	}
	counterGroups := []mapreduce.CounterGroup{
		{
			CounterGroupName: "org.test.group.a",
			Counters: []mapreduce.Counter{
				{Name: "counter1", TotalCounterValue: 1},
				{Name: "counter2", TotalCounterValue: 2},
				{Name: "counter3", TotalCounterValue: 3}},
		},
		{
			CounterGroupName: "org.test.group.b",
			Counters: []mapreduce.Counter{
				{Name: "counter1", TotalCounterValue: 1},
				{Name: "counter2", TotalCounterValue: 2}},
		},
		{
			CounterGroupName: "org.test.group.c",
			Counters: []mapreduce.Counter{
				{Name: "counter1", TotalCounterValue: 1},
				{Name: "counter2", TotalCounterValue: 2}},
		},
	}

	err := SetMapReduceEventFields(event, counterConfig, &job, counterGroups)
	if err != nil {
		t.Error("Failed to set mapreduce event fields: ", err)
	}

	valueTable := map[string]interface{} {
		"mapreduce.job.counters.a.counter_1": uint64(1),
		"mapreduce.job.counters.a.counter_2": uint64(2),
		"mapreduce.job.counters.b.counter_1": uint64(1),
		"mapreduce.job.counters.b.counter_2": uint64(2),
		"mapreduce.job.id": "job_1234",
		"mapreduce.job.name": "test job",
	}

	for k, tv := range valueTable {
		if v, _ := event.Fields.GetValue(k); v == nil || v != tv {
			t.Errorf("Counters value '%s' is %s, expected %s",k, v, tv)
		}
	}
}
