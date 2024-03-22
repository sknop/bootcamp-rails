import argparse
import sys
from configparser import ConfigParser
from pprint import pformat
import logging

from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition, OFFSET_INVALID, OFFSET_BEGINNING
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from global_land_mask import globe


def setup_logger(name) -> logging.Logger:
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    return logger


class LocationChecker:
    def __init__(self, configfile, topic_name, attribute):
        self.config_file = configfile
        self.topic_name = topic_name
        self.attribute = attribute
        self.running = True

        self.logger = setup_logger('location-checker')

        config = ConfigParser()
        with open(self.config_file, 'r') as f:
            content = f.read()

        content = '[top]\n' + content
        config.read_string(content)

        self.config = {k: v for k, v in config['top'].items()}
        if 'group.id' not in self.config:
            self.config['group.id'] = 'location.checker'
        if 'auto.offset.reset' not in self.config:
            self.config['auto.offset.reset'] = 'earliest'

        if 'schema.registry.url' in self.config:
            schema_registry_config = {'url': self.config.pop('schema.registry.url')}
            self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
            self.avro_deserializer = AvroDeserializer(self.schema_registry_client)

        self.logger.info(pformat(self.config))

    def load_data(self):
        consumer = Consumer(self.config)

        partitions = self.get_partitions(consumer)
        # Reset the consumer group to start from the beginning, rather than creating a new one every time
        reset_partitions = [TopicPartition(part.topic, part.partition,OFFSET_BEGINNING) for part in partitions]
        consumer.assign(reset_partitions)

        try:
            # assign is a specific form of subscribe, and subscribe would reset my groups
            # consumer.subscribe([self.topic_name])

            while self.running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                        self.running = False
                    else:
                        raise KafkaException(msg.error())
                else:
                    self.process_message(msg)
        except KeyboardInterrupt:
            print("Interrupted, closing connection.")
        finally:
            consumer.close()

    def process_message(self, msg):
        message = self.avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        # self.logger.debug(message)

        name = message["NAME"]
        description = message["DESCRIPTION"]
        location_id = message["LOCATION_ID"]
        stanox = message["STANOX"]
        crs = message["CRS"]
        nlc = message["NLC"]
        lat_lon = message["LAT_LON"]
        lat = lat_lon["lat"]
        lon = lat_lon["lon"]
        isoffnetwork = message["ISOFFNETWORK"]

        if lat is None or lon is None or globe.is_land(lat, lon):
            pass  # print(f"{name}: {lat} {lon}")
        elif globe.is_ocean(lat, lon):
            print(f"Ocean: {name} {description} {location_id} {stanox} {crs} {nlc} {isoffnetwork} with lat={lat} lon={lon}")
        else:
            print("***** So what is it? *****")

    def get_partitions(self, consumer):
        print("%-50s  %9s  %9s" % ("Topic [Partition]", "Committed", "Lag"))
        print("=" * 72)

        # Get the topic's partitions
        metadata = consumer.list_topics(self.topic_name, timeout=10)
        if metadata.topics[self.topic_name].error is not None:
            raise KafkaException(metadata.topics[self.topic_name].error)

        # Construct TopicPartition list of partitions to query
        partitions = [TopicPartition(self.topic_name, p) for p in metadata.topics[self.topic_name].partitions]

        self.logger.debug(partitions)

        # Query committed offsets for this group and the given partitions
        committed = consumer.committed(partitions, timeout=10)

        for partition in committed:
            # Get the partitions low and high watermark offsets.
            (lo, hi) = consumer.get_watermark_offsets(partition, timeout=10, cached=False)

            if partition.offset == OFFSET_INVALID:
                offset = "-"
            else:
                offset = "%d" % partition.offset

            if hi < 0:
                lag = "no hwmark"  # Unlikely
            elif partition.offset < 0:
                # No committed offset, show total message count as lag.
                # The actual message count may be lower due to compaction
                # and record deletions.
                lag = "%d" % (hi - lo)
            else:
                lag = "%d" % (hi - partition.offset)

            print("%-50s  %9s  %9s" % (
                "{} [{}]".format(partition.topic, partition.partition), offset, lag))

        return partitions


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="LocationChecker")

    parser.add_argument('-c', '--config', help="Kafka Configuration File", required=True)
    parser.add_argument('-t', '--topic', help="Topic to check for location data", required=True)
    parser.add_argument('-a', '--attribute', help="The attribute to check", required=True)

    args = parser.parse_args()

    checker = LocationChecker(args.config, args.topic, args.attribute)

    checker.load_data()
