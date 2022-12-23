from urllib.parse import urlencode
from params import *
import taos
import logging
import random
import time
import base64
from taos.tmq import *


def cmdline_consumer():
    parser = get_parser()
    parser.add_argument('--use_case')
    parser.add_argument('--consumer_group_name', default = 'group')
    parser.add_argument('--consumer_group_prefix', default = 'cg')
    args = parser.parse_args()
    # logger
    logger = get_logger("consumer", args.log_level)

    logger.info("Prepare to Connect ...")
    conn = taos.connect(hostname = args.hostname, port = args.port)
    logger.info("TDengine connected")
    try:
        conn.execute("USE %s ;" % args.database)
    except Exception as e:
        logger.error("Meet %s", e)
        print(e.__class__.__name__)
        print(e.errno)
        print(e.msg)        
        exit(-1)
    # topic name
    if args.key is None:
        topic_name = "%s_%s" % (args.topic_prefix, args.topic)
        target_name = "%s_%s" % (args.stable_prefix, args.topic)
    else:
        key_str = urlencode({"k": args.key})
        topic_name = "%s_%s" % (args.topic_prefix, key_str )
        target_name = "%s_%s" % (args.table_prefix, )
    # Create Topic
    conn.execute("CREATE TOPIC IF NOT EXISTS %s AS SELECT * FROM `%s`.`%s`" % (
        topic_name, args.database, target_name))
    # Create consumer group
    # conn.execute("CREATE CONSUMER GROUP IF NOT EXISTS %s_%s ON `%s`" %(
    #     args.consumer_group_prefix, args.consumer_group_name, topic_name
    # ))
    # Create Consumer
    try:
        client_id = "client_%d" % random.randint(0,1000)
        conf = TaosTmqConf()
        conf.set('client.id', client_id)
        conf.set('group.id', "%s_%s" %(args.consumer_group_prefix, args.consumer_group_name))
        consumer = conf.new_consumer()
        topic_list = TaosTmqList()
        topic_list.append('%s' % topic_name)
        consumer.subscribe(topic_list)
        while True:
            res = consumer.poll(1000)
            if res:
                topic = res.get_topic_name()
                vg = res.get_vgroup_id()
                db = res.get_db_name()
                for row in res:
                    print("vgid=%d,key=%s, ts=%s, v=%s" %(vg, row[-1], row[0], row[1]))
                    # print("key=%s, [%s, %s]" %(row[-1], row[0], row[1]))
                    # print(type(row))
                    # print(row)
    except (KeyboardInterrupt, EOFError):
        print()
        logger.info("Close Terminal...")
        consumer.unsubscribe()
        conn.close()


if '__main__' == __name__:
    cmdline_consumer() 