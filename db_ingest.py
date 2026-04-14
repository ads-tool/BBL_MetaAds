import pandas as pd
import psycopg2
import json
import re
import ast

def ingest_excel_to_postgres(excel_path: str, db_config: dict):
    df = pd.read_excel(excel_path)
    df = df.fillna("N/A")

    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    ad_group_cache = {}

    for _, row in df.iterrows():
        countries_raw = str(row.get('countries', 'N/A'))
        if countries_raw != 'N/A':
            countries_list = [c.strip().upper() for c in countries_raw.split(',') if c.strip()]
        else:
            countries_list = []

        if len(countries_list) == 1 and len(countries_list[0]) >= 2:
            loc = countries_list[0][:2] 
        else:
            loc = 'WW'

        gender_raw = str(row.get('gender_audience', 'ALL')).strip().upper()
        if gender_raw in ['MALE', 'M', 'NAM']:
            gender = 'M'
        elif gender_raw in ['FEMALE', 'F', 'NỮ']:
            gender = 'F'
        else:
            gender = 'ALL'

        age_raw = row.get('age_audience', 'N/A')
        age = age_raw
        
        if pd.isna(age_raw) or age_raw == 'N/A' or str(age_raw).strip() == '':
            age = 'NS'
        else:
            age_str = str(age_raw).strip()
            if '{' in age_str:
                try:
                    age_dict = ast.literal_eval(age_str.replace('null', 'None'))
                    
                    min_age = age_dict.get('min', 'NS')
                    max_age = age_dict.get('max', 'NS')
                    age = f"{min_age}-{max_age}"
                except Exception:
                    age = age_str.replace(' ', '')
            else:
                age = age_str.replace(' ', '')
        ad_group_name = f"{loc}_{gender}_{age}"
        if ad_group_name not in ad_group_cache:
            cur.execute("""
                INSERT INTO ad_groups (name, gender_audience, age_audience)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    gender_audience = EXCLUDED.gender_audience,
                    age_audience = EXCLUDED.age_audience
                RETURNING id;
            """, (ad_group_name, row['gender_audience'], row['age_audience']))
            
            ad_group_cache[ad_group_name] = cur.fetchone()[0]
            
        ad_group_id = ad_group_cache[ad_group_name]

        duration = None
        if row.get('duration', 'N/A') != 'N/A':
            try: duration = float(row['duration'])
            except: pass

        dup_count = 0
        dup_val = row.get('duplicate_count', 0)
        if dup_val != 'N/A':
            try:
                dup_count = int(float(dup_val)) 
            except:
                pass

        cur.execute("""
            INSERT INTO videos (
                video_url, duration, transcript, transcript_translated, 
                video_language, duplicate_count, top3_reach
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
        """, (
            row['video_url'], duration, row['transcript'], 
            row['transcript_translated'], row['video_language'], 
            dup_count, row['top3_reach']
        ))
        video_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO texts (headline, headline_language, primary_text, primary_text_language)
            VALUES (%s, %s, %s, %s) RETURNING id;
        """, (
            row['headline'], row['headline_language'], 
            row['primary_text'], row['primary_text_language']
        ))
        text_id = cur.fetchone()[0]

        countries_list = [c.strip() for c in str(row['countries']).split(',')] if row.get('countries', 'N/A') != 'N/A' else []
        
        crawl_date = None
        if row.get('crawl_date', 'N/A') != 'N/A':
            crawl_date = str(row['crawl_date']).split(' ')[0]

        data_source = 'facebook'

        reach_val = None
        if row.get('reach (EU)', 'N/A') != 'N/A':
            nums = re.findall(r'\d+', str(row['reach (EU)']).replace(',', ''))
            if nums: 
                reach_val = int(nums[0])
        
        cur.execute("""
            INSERT INTO adsets (
                ad_group_id, video_id, text_id, ad_id_full, library_id_full,
                crawl_date, countries, reach, cta_text, cta_type, app_link,
                source_input_kind, source_input_value, data_source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (data_source, ad_id_full, crawl_date) DO UPDATE SET
                ad_group_id = EXCLUDED.ad_group_id,
                video_id = EXCLUDED.video_id,
                text_id = EXCLUDED.text_id,
                countries = EXCLUDED.countries,
                reach = EXCLUDED.reach,
                cta_text = EXCLUDED.cta_text,
                cta_type = EXCLUDED.cta_type,
                app_link = EXCLUDED.app_link;
        """, (
            ad_group_id, video_id, text_id, row['ad_id_full'], row.get('library_id_full'),
            crawl_date, json.dumps(countries_list), reach_val, row.get('cta_text'), row.get('cta_type'), 
            row.get('app_link'), row.get('source_input_kind'), row.get('source_input_value'),
            data_source
        ))
    conn.commit()
    cur.close()
    conn.close()