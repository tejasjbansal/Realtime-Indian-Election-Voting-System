import random

import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer
from constants import *


# Function to create candidates table
def create_candidates_table(conn, cur):
    # SQL query to create candidates table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    conn.commit()

# Function to create voters table
def create_voters_table(conn, cur):
    # SQL query to create voters table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)
    conn.commit()

# Function to create votes table
def create_votes_table(conn, cur):
    # SQL query to create votes table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    conn.commit()

# Function to retrieve candidates from the database
def get_candidates_from_db(cur):
    cur.execute("SELECT * FROM candidates")
    candidates = cur.fetchall()
    print(candidates)
    return candidates

def generate_candidate_data(candidate_number):
    
    return {
        "candidate_id": CANDIDATES_ID[candidate_number],
        "candidate_name": CANDIDATES[candidate_number],
        "party_affiliation": PARTIES[candidate_number],
        "biography": BIOGRAPHY[candidate_number],
        "campaign_platform": "Key campaign promises or platform.",
        "photo_url": PHOTO_URL[candidate_number]
    }

# Function to insert initial candidates if none exist in the database
def insert_initial_candidates(conn, cur):
    for i in range(4):
        candidate = generate_candidate_data(i)
        print(candidate)
        cur.execute("""
                    INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
            candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
            candidate['campaign_platform'], candidate['photo_url']))
        conn.commit()

def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"

def insert_voters(conn, cur, voter):
    cur.execute("""
                        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Function to produce voters and send them to Kafka topic
def produce_voters(conn, cur, producer, voters_topic):
    for i in range(1000):
        voter_data = generate_voter_data()
        insert_voters(conn, cur, voter_data)

        producer.produce(
            voters_topic,
            key=voter_data["voter_id"],
            value=json.dumps(voter_data),
            on_delivery=delivery_report
        )

        print('Produced voter {}, data: {}'.format(i, voter_data))
        producer.flush()

if __name__ == "__main__":
    try:
        # Establish connection to PostgreSQL database
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        # Initialize Kafka producer
        producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })

        # Kafka Topics
        voters_topic = 'voters_topic'
        candidates_topic = 'candidates_topic'

        # Create database tables if not exist
        create_candidates_table(conn, cur)
        create_voters_table(conn, cur)
        create_votes_table(conn, cur)

        # Retrieve candidates from the database
        candidates = get_candidates_from_db(cur)

        # If no candidates exist in the database, insert initial candidates
        if len(candidates) == 0:
            insert_initial_candidates(conn, cur)
        
        produce_voters(conn, cur, producer, voters_topic)
        
    except Exception as e:
        print(e)