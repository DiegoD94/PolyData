import pandas as pd
import datetime
from pytz import timezone
import requests
from tqdm.auto import tqdm
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor,  as_completed


def is_weekend(date_obj):
    is_saturday = date_obj.weekday() == 5  # Saturday
    is_sunday = date_obj.weekday() == 6    # Sunday
    return is_saturday or is_sunday
def get_difference(start_date, end_date, span):
    delta = end_date - start_date
    if span == 'day':
        days_difference = delta.days
        return days_difference
    elif span == 'minute':
        seconds_difference = delta.total_seconds()
        minutes_difference = seconds_difference / 60
        return minutes_difference
    elif span == 'second':
        seconds_difference = delta.total_seconds()
        return seconds_difference
    else:
        assert False, "Wrong span"
        
        
def get_query_strings(ticker, start_date, end_date, span, multiplier, prepost_market=False, limit = 30000, api_key="NahcgY5iZpJiLbXPYqZcgV6RatP09HUV"):
    if type(start_date) == str:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    if type(end_date) == str:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    if span == 'second':
        seconds_per_segment = 24 * 3600
    if span == 'minute':
        seconds_per_segment = 24 * 3600 * 30
    if span == 'day':
        seconds_per_segment = 24 * 3600 * 365
    difference = get_difference(start_date, end_date, "second")
    n_segment = int(difference // seconds_per_segment) + 1
    segments = [start_date + timedelta(seconds=i * seconds_per_segment) for i in range(n_segment + 1) if start_date + timedelta(seconds=i * seconds_per_segment) < end_date]
    segments.append(end_date+timedelta(days=1))
    querys = []
    for s, e in zip(segments[:-1], segments[1:]):
        if s.weekday() == 5 and (e-s).days <= 2:
            continue
        if s.weekday() == 6 and (e-s).days <= 1:
            continue
        from_date = s.strftime("%Y-%m-%d")
        to_date = (e-timedelta(days=1)).strftime("%Y-%m-%d")
        query = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{span}/{from_date}/{to_date}?adjusted=true&sort=asc&limit={limit}&apiKey={api_key}"
        querys.append(query)
    return querys
def post_request(query_string):
    try:
        res_df = pd.DataFrame().from_records(requests.get(query_string).json()['results'])
        res_df['datetime'] = res_df['t'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000, timezone('US/Eastern')).strftime('%Y-%m-%d-%H:%M:%S'))
        res_df['date'] = res_df['datetime'].apply(lambda x:'-'.join(x.split('-')[:3]))
        res_df['time'] = res_df['datetime'].apply(lambda x:'-'.join(x.split('-')[3:]))
        return res_df
    except requests.exceptions.RequestException as e:
        print(str(e))
    except KeyError as e2:
        print(str(e2), print(query_string))


if __name__ == "__main__":
    api_key = "NahcgY5iZpJiLbXPYqZcgV6RatP09HUV"
    from_date = "2024-01-01"
    to_date = "2024-04-06"
    querys = get_query_strings("QQQ", from_date, to_date, 'minute', 1)
    results = []
    with tqdm(total=len(querys)) as pbar:
        with ThreadPoolExecutor(max_workers=16) as ex:
            futures = [ex.submit(post_request, q) for q in querys]
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pbar.update(1)
    res_df = pd.concat(results).sort_values(by='t', ascending=True).reset_index(drop=True)
    print(res_df.head(10))
    
