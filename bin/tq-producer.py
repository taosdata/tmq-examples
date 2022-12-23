from urllib.parse import urlencode
from params import *
import taos
import random
import time
import base64

def cmdline_producer():
    parser = get_parser()
    parser.add_argument('--max_message_length', default = 1024)
    parser.add_argument('--use_case')
    parser.add_argument('--sharding', default = 4, type = int)
    args = parser.parse_args()

    # logger
    logger = get_logger("producer", args.log_level)
    logger.info("Prepare to Connect ...")
    conn = taos.connect(hostname = args.hostname, port = args.port)
    logger.info("TDengine connected")
    try:
        conn.execute("CREATE DATABASE %s VGROUPS %d" % (args.database, args.sharding))
        conn.execute("USE %s ;" % args.database)
    except Exception as e:
        if -2147482751 == e.errno:
            logger.warning("Database %s already exists" % args.database)
            conn.execute("USE %s ;" % args.database)
        else:
            logger.error("Meet %s", e)
            print(e.__class__.__name__)
            print(e.errno)
            print(e.msg)        
            exit(-1)
    conn.execute("USE %s ;" % args.database)
    conn.execute("CREATE STABLE IF NOT EXISTS %s_%s (ts TIMESTAMP, msg BINARY(%d) ) TAGS (key BINARY(64));" % (
        args.stable_prefix, args.topic, args.max_message_length))
    logger.info("Stable %s_%s created", args.stable_prefix, args.topic)
    if args.key is None:
        args.subtopic = 'test_key'
    # Create topic
    conn.execute("CREATE TOPIC IF NOT EXISTS %s_%s AS SELECT * FROM %s_%s " % (
        args.topic_prefix, args.topic, args.stable_prefix, args.topic) )
    printable = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    if args.key is None and 'randomMsg' == args.use_case:
        args.key = "".join([random.choice(printable) for x in range(6)])
    while True:
        try:
            if 'randomMsg' == args.use_case:
                input("Press Enter to send a random message.")
                key = "car_%d" % random.randint(0, args.sharding * 10)
                # ctx = "".join([random.choice(printable) for x in range(int(random.randint(0,args.max_message_length - 1)))])
                ctx = "".join([random.choice(printable) for x in range(int(random.randint(3,8)))])
            else:
                key = input("Input key:")
                ctx = input("Input message: ")
            tbname = '`%s_%s`' % (args.table_prefix, urlencode({"k": key}))
            sql = "INSERT INTO %s USING %s_%s TAGS ('%s') VALUES(NOW, '%s');" % (tbname, args.stable_prefix, args.topic, key, ctx)
            logger.debug("SQL: %s" % sql)
            ts_before = time.time()
            conn.execute(sql)
            ts_after = time.time()
            logger.info("INSERT 1 record takes %f ms",(ts_after - ts_before) * 1000)
        except (KeyboardInterrupt, EOFError):
            print()
            logger.info("Close Terminal...")
            conn.close()
            break
        except Exception as e:
            print(e.__class__.__name__)
            print(e.msg)
            conn.close()




if '__main__' == __name__:
    cmdline_producer() 