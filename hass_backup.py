import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()
db_user=os.getenv("sql_user")
db_pw=os.getenv("sql_password")
db_host=os.getenv("db_host")
db_port= "5432"
db_name="hass_db"

# Establish a connection to the database
conn = psycopg2.connect(
    host=db_host,
    database="hass_db",
    user=db_user,
    password=db_pw
)

# Create a cursor object
cur = conn.cursor()

# Create an engine to connect to PostgreSQL database
engine =create_engine(f"postgresql+psycopg2://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}")

# Query AC runtime from live database and save to the backup database
ac_runtime_sql="""
    SELECT 
      DATE(to_timestamp(s.last_updated_ts)) AS ac_date,
	  UPPER(SUBSTRING(sm.entity_id from 8 for 2)) AS ac_location,
	  INITCAP(split_part(sm.entity_id,'_',2)) AS ac_action,
      max(state::numeric) AS ac_hours,
	  now() AS ac_created,
	  TO_CHAR(DATE(to_timestamp(s.last_updated_ts))::Date, 'YYYY/FMMM/FMDD') || '_' || UPPER(SUBSTRING(sm.entity_ID from 8 for 2)) || '_' ||INITCAP(split_part(sm.entity_ID,'_',2))  as ac_id
    FROM public.states s
    LEFT JOIN public.states_meta sm on sm.metadata_id=s.metadata_id
    WHERE state is not null and state<>'unknown' 
      and state<>'unavailable' and state<>'' 
      and state<>'backed_up'
      and s.metadata_id in (1,2,3,4,5)
	  and DATE(to_timestamp(s.last_updated_ts)) < current_date 
	  and DATE(to_timestamp(s.last_updated_ts)) > (SELECT max(b.ac_date) FROM backup_ac_runtime b)
    GROUP BY sm.entity_id, ac_action, ac_location, ac_date
    HAVING max(state::numeric) > 0
    """

cur.execute(ac_runtime_sql)
ac_runtime_results = cur.fetchall()
df_ac_runtime = pd.DataFrame(ac_runtime_results,columns=[desc[0] for desc in cur.description])
df_ac_runtime.to_sql('backup_ac_runtime', engine, if_exists='append', index=False)

# Query temperature history from live database and save to the backup database
temp_sql="""
    SELECT date(to_timestamp(s.last_updated_ts)) AS temp_date,
        EXTRACT(hour FROM to_timestamp(s.last_updated_ts)) AS temp_hour,
        max(s.state::numeric) AS temp_max,
        min(s.state::numeric) AS temp_min,
        round(avg(s.state::numeric), 2) AS temp_mean,
        sm.entity_id,
        now() AS temp_created,
        (((to_char(date(to_timestamp(s.last_updated_ts))::timestamp with time zone, 'YYYY/FMMM/FMDD'::text) || '_'::text) || EXTRACT(hour FROM to_timestamp(s.last_updated_ts))) || '_'::text) || sm.entity_id::text AS temp_id
    FROM states s
    LEFT JOIN states_meta sm ON sm.metadata_id = s.metadata_id
    WHERE s.state IS NOT NULL 
      AND s.state::text <> 'unknown'::text 
      AND s.state::text <> 'unavailable'::text 
      AND s.state::text <> ''::text 
      AND s.state::text <> 'backed_up'::text 
      AND (sm.metadata_id = ANY (ARRAY[21,23,30,36,45,70,237,257,263])) 
	  AND s.state ~ '^[0-9]+(\.[0-9]+)?$' -- Regex check to ensure s.state is numeric
      AND s.state::numeric >= 0::numeric AND s.state::numeric <= 150::numeric 
      AND date(to_timestamp(s.last_updated_ts)) < CURRENT_DATE 
      AND date(to_timestamp(s.last_updated_ts)) > (( SELECT max(backup_temp_history.temp_date) AS max FROM backup_temp_history))
    GROUP BY sm.entity_id, (date(to_timestamp(s.last_updated_ts))), (EXTRACT(hour FROM to_timestamp(s.last_updated_ts)));
    """

cur.execute(temp_sql)
temp_results = cur.fetchall()
df_temp = pd.DataFrame(temp_results,columns=[desc[0] for desc in cur.description])
df_temp.to_sql('backup_temp_history', engine, if_exists='append', index=False)

cur.close()
conn.close()
