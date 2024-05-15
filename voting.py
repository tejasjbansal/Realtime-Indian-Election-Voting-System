import random
import time
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

from main import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)

# Function to retrieve candidates from the database
def get_candidates_from_db(cur):
    cur.execute("""
            SELECT row_to_json(t)
            FROM (
                SELECT * FROM candidates
            ) t;
        """)
    candidates = cur.fetchall()
    return candidates

def process_message(msg, cur, conn, candidates):
    if msg is None:
        return
    elif msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            return
        else:
            print(msg.error())
            return
    else:
        voter = json.loads(msg.value().decode('utf-8'))
        chosen_candidate = random.choice(candidates)
        vote = {
            **voter,
            **chosen_candidate,
            "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "vote": 1
        }

        try:
            print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
            cur.execute("""
                    INSERT INTO votes (voter_id, candidate_id, voting_time)
                    VALUES (%s, %s, %s)
                """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

            conn.commit()

            producer.produce(
                'votes_topic',
                key=vote["voter_id"],
                value=json.dumps(vote),
                on_delivery=delivery_report
            )
            producer.poll(0)
        except Exception as e:
            print("Error: {}".format(e))
            # conn.rollback()
            return


def consume_and_process(consumer, cur, conn, candidates):
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            process_message(msg, cur, conn, candidates)
            time.sleep(0.2)
    except KafkaException as e:
        print(e)

if __name__ == "__main__":

    try:

        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        candidates = get_candidates_from_db(cur)
        candidates = [candidate[0] for candidate in candidates]
        if len(candidates) == 0:
            raise Exception("No candidates found in database")
        else:
            print(candidates)

        consumer.subscribe(['voters_topic'])
        consume_and_process(consumer,cur,conn,candidates)
        

    except Exception as e:
        print("Error: {}".format(e))