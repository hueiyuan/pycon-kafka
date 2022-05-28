# pycon-kafka
The repo is kafka and schema registry basic example in python for PyCon APAC 2022

## How to use ?
### Step 1. Please execute cli to install required libs.
```
pip3 install -r requirements.txt
```

### Step2. Execute setup.py install to install custom helpers. (> python3.8)
```
python3 setup.py install
```

### Step3. Please check your kafka and schema registry service is running.

### Step4. Please update schema registry auth username and password in `.env` file.
```
SCHEMA_REGISTRY_USERNAME=your-schema-registry-username
SCHEMA_REGISTRY_PASSWORD=your-schema-registry-password
```

### Step5. Execute and emulate producer of the kafka.
```
python3 producer_entrypoint.py --schema-registry-endpoint http://schema-registry-endpoint.xxx.com:8080 --kafka-brokers broker1,broker2
```

### Step6. Execute and emulate consumer of the kafka.
```
python3 consumer_entrypoint.py --schema-registry-endpoint http://schema-registry-endpoint.xxx.com:8080 --kafka-brokers broker1,broker2 --consumer-group-id your-consumer-group-id
```

## What is the Next?
Since the repo is a basic repo and provide a simple data and one topic of kakfa to emulate. 
If you are familiar with operate and logics about producer, consumer and schema registry. You can fork this repo and change **dynamic data** and **multiple topics** to ingest or consume data to kafka with schema registry.

## Reference
1. [Schema Registry Overview](https://docs.confluent.io/platform/current/schema-registry/index.html)
2. [Schema Registry Download and install](https://docs.confluent.io/platform/current/schema-registry/installation/index.html)
3. [confluent kafka client in python](https://github.com/confluentinc/confluent-kafka-python)

