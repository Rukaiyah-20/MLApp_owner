import json
import urllib.request
from datetime import datetime, timedelta
import pandas as pd
import pandas_gbq


def concurrent_plays():
    table_id = "conviva_session_poc.concurrentplays_conviva_ott"
    project_id = "skyuk-uk-anp-dateng-pilot-poc"
    publisher = pubsub_v1.PublisherClient()
    topic_id = "test_demo"
    topic_path = publisher.topic_path(project_id, topic_id)
    credentials = service_account.Credentials.from_service_account_file('nightswatch_poc.json')
    region = ['de', 'roi', 'uk', 'it']
    proposition = ['now-tv', 'sky-go', 'sky-q']

    base = datetime(2000, 1, 1)

    subtracted_date = pd.to_datetime('today') - timedelta(1)
    subtracted_date = subtracted_date.strftime('%Y-%m-%d')
    for reg in region:
        for prop in proposition:
            url = f'https://ott-insights-api-internal.dev.sky.aieng.ottinsights.sky/api/v1/metrics/concurrent-plays,timestamp?start={subtracted_date}T00:00:00Z&end={subtracted_date}T23:59:55Z&territory={reg}&proposition={prop}'
            print(url)
            with urllib.request.urlopen(url) as url:
                data = json.load(url)
                print(data)
            if data['data']['total']:
                print(f'json {prop}_{reg}_parsed correctly')
                for key, values in data['data'].items():
                    df = pd.DataFrame.from_records(values)
                df['propositions'] = reg + '_' + prop
                df['propositions'] = df['propositions'].astype(str)
                for column in df[['timestamp']]:
                    columnSeriesObj = df[column]
                    df['timestamp'] = columnSeriesObj.values
                    # print(df)
                print(df.dtypes)
            else:
                print(f'json {prop}_{reg}_has no data')

            pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists="append")

def write_to_pubsub(data):
    try:
        publisher.publish(topic_path, data=json.dumps(data).encode("utf-8"))
    except:
        raise

concurrent_plays()
