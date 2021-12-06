"""
Author: kamrul Hasan
Date: 05.12.2021
Email: hasan.alive@gmail.com
"""
import os.path

from confluent_kafka import Producer
import uuid
import json
import time
import random
import utlilities.utility_factory


class KafkaProducer:
    def __init__(self, bootsrap_server) -> None:
        self.bootsrap_server = bootsrap_server

    @utlilities.utility_factory.DecoratorFactory.program_status
    def produce_msg(self, topic, data_all) -> None:
        """
        The prodicer method to produce message from batch or stream session to different kafka topic.
        Here i used the two given test batch file to produce two topics with the data.
        :param topic: str
        :param data_all: json
        :return: None
        """
        p2 = Producer({'bootstrap.servers': self.bootsrap_server})

        for data in data_all:
            # Trigger any available delivery report callbacks from previous produce() calls
            p2.poll(0)

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            p2.produce(topic, json.dumps(data).encode('utf-8'), callback=self.delivery_report)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.

        p2.flush()

    @staticmethod
    def read_data_offline() -> object:
        """
        Reading the test data file from the specific location
        :return: Json object
        """

        data_one =config_data['workorder_data']
        data_two = config_data['metrics_data']
        path = './../resources/test_data/'

        with open(os.path.join(path,data_one)) as f:
            wo_data = json.load(f)

        with open(os.path.join(path,data_two)) as f:
            met_data = json.load(f)

        return wo_data, met_data

    @staticmethod
    def delivery_report(err, msg) -> None:
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == "__main__":
    config_data = utlilities.utility_factory.load_config()
    prodcuer = KafkaProducer('localhost:29092')
    wo_data, met_data = prodcuer.read_data_offline()
    prodcuer.produce_msg('machine_response_wo_fp', wo_data)
    prodcuer.produce_msg('machine_response_met_fp', met_data)
