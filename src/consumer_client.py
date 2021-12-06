from multiprocessing import Process
import uuid
from confluent_kafka import Consumer, TopicPartition
import pandas as pd
import re
import utlilities.utility_factory
from src.db_engine import DBFactory

"""
Consumer module will run concurrently two kafka consumer as we have two kafka topics which are recieving message from two stream source.
"""


def consume_data(topic) -> None:
    """
    The consumer function to consume the data from different topic. The consumer will be keep running looking for
    data stream in the kafka topic it subscribed to.Consumer will take the data from earliest offset. Then the data
    will be dumped directly into postgres table.
    :param topic: str
    :return: None
    """

    # set the config for consumer
    try:
        c = Consumer({
            'bootstrap.servers': 'localhost:29092',
            'group.id': str(uuid.uuid1()),
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'earliest'}
        })

        # subscribe to a particular topic
        c.subscribe([topic])
    except:
        print("Error!!")

    config_data = utlilities.utility_factory.load_config()

    db_instance = DBFactory(config_data['db_hostname'], config_data['db_port'], config_data['db_name'],
                            config_data['db_username'], config_data['db_password'])
    db_instance.connect()

    cursor = db_instance.conn.cursor()

    # define the colum name
    collist_wo = ["time", "product", "production"]
    collist_met = ["id", "val", "time"]

    while True:
        msg = c.poll(0)
        if msg is None:
            continue

        if msg.error():
            print("Consumer error: {}".format(msg.error()))  # for debugging
            continue

        # do some transformation
        clean_msg = msg.value().decode("utf-8")
        scmsg = clean_msg.split(",")
        nms_wo = [sub.split(":")[1] for sub in scmsg]

        if topic == 'machine_response_met_fp':
            df = pd.DataFrame(list(map(int, [re.sub('[^a-zA-Z0-9]+', '', _) for _ in nms_wo]))).transpose()
            df.columns = collist_met
            df.reset_index(drop=True, inplace=True)
            for i, d in df.iterrows():
                cursor.execute("INSERT INTO metrics_data (param_id, val, time) VALUES (%s,%s,%s)",
                               (int(d['id']), int(d['val']), int(d['time'])))
                db_instance.conn.commit()
        else:
            df = pd.DataFrame(list(map(float, [re.sub("[^\\d.]", '', _) for _ in nms_wo]))).transpose()
            df.columns = collist_wo
            df.reset_index(drop=True, inplace=True)
            for i, d in df.iterrows():
                cursor.execute("INSERT INTO worknode_data (time, product, production) VALUES (%s,%s,%s)",
                               (int(d['time']), int(d['product']), (d['production'])))
                db_instance.conn.commit()


if __name__ == '__main__':
    config_data = utlilities.utility_factory.load_config()
    # create databse instance
    db_instance = DBFactory(config_data['db_hostname'], config_data['db_port'], config_data['db_name'],
                            config_data['db_username'], config_data['db_password'])

    # create two separate table to use as a dummy kafka sink

    db_instance.create_table("worknode_data")
    db_instance.create_table("metrics_data")

    # start multiprocessing to simultaneously fetch data from two kafka topics
    t1 = Process(target=consume_data, args=('machine_response_met_fp',))
    t2 = Process(target=consume_data, args=('machine_response_wo_fp',))
    t1.start()
    t2.start()
