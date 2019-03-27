# Yarnbeat
[![Build Status](https://travis-ci.org/IBM/yarnbeat.svg?branch=master)](https://travis-ci.org/IBM/yarnbeat)

Yarnbeat is an elastic [Beat](https://www.elastic.co/products/beats) which polls the [YARN API](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html) and forwards YARN application and resource metric information to the ELK stack for monitoring and alerting.

## Getting Started

### Prerequisites
Yarnbeat was developed against Go 1.12. Other versions of Go may work, but I make no promises.

### Configuration

Example yarnbeat configuration:

```yaml
yarnbeat:
  # Defines how often job metrics are polled in seconds
  period: 60
  
  # List of resource manager URLs to try when polling the YARN API
  # For best performance, list our primary RM first, then failovers
  resource_manager_urls:
  - "http://localhost:8088"

  # Set to true to enable MapReduce History Server integration
  enable_mr: true
  history_server_url: "http://localhost:9888"
  
  # Configure which MapReduce counters should be included in the
  # output, and what their field names should be
  mr_counters:
    - group_name: org.apache.hadoop.mapreduce.TaskCounter
      counters:
        # The format here is COUNTER_NAME: field_name
        # counter fields will appear as "mapreduce.job.counters.field_name"
        MAP_INPUT_RECORDS: map_input_records
        MAP_OUTPUT_RECORDS: map_output_records
```

For information concerning valid Beats outputs, see the [Filebeat documentation](https://www.elastic.co/guide/en/beats/filebeat/6.6/configuring-output.html)

### Build
To build Yarnbeat from source,  clone the repository and execute the following command:
```bash
make yarnbeat
```

### Run
To execute Yarnbeat after building from source, run
```bash
./bin/yarnbeat -c /path/to/yarnbeat.yml
```

To execute Yarnbeat directly with Go, run
```bash
go run github.com/IBM/yarnbeat -c /path/to/yarnbeat.yml
```

For full command options run
```bash
./bin/yarnbeat --help
```

### Test
To test the Yarnbeat module run
```bash
make test
```

### Build and run the Docker image
To run Yarnbeat in a Docker image, mount the directory containing `yarnbeat.yml` to `/etc/yarnbeat`
```bash
make docker
docker run -v /path/containing/yarnbeatyml:/etc/yarnbeat yarnbeat
```

## Kibana Dashboards
To load the prebuilt objects (index pattern, visualizations, dashboards) into Kibana, execute the following commands.  Note that your config file must contain valid connection settings in `setup.kibana`.
```bash
make yarnbeat
make dashboards
./bin/yarnbeat setup --dashboards -c path/to/yarnbeat.yml
```