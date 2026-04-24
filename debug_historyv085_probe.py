import json
import sqlite3
import requests
from core.database import decrypt_token

DB = '/opt/mira/data/mira.db'
PAGE_ID = '116581311406362'
TOKEN_IDS = [1, 18, 19, 20]
FB = 'https://graph.facebook.com/v25.0'

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT id, token_alias, access_token_enc FROM fb_tokens WHERE id IN (1,18,19,20) ORDER BY id").fetchall()
conn.close()

for row in rows:
    token = decrypt_token(row['access_token_enc'])
    print(f'=== token {row["id"]} {row["token_alias"]} ===')
    try:
        me = requests.get(f'{FB}/me', params={'access_token': token, 'fields': 'id,name'}, timeout=20).json()
        print('me:', json.dumps(me, ensure_ascii=False))
        pages = requests.get(f'{FB}/me/accounts', params={'access_token': token, 'fields': 'id,name,access_token,tasks', 'limit': 200}, timeout=20).json()
        found = None
        for p in pages.get('data', []) or []:
            if str(p.get('id')) == PAGE_ID:
                found = p
                break
        print('page_found:', json.dumps(found, ensure_ascii=False))
        if found and found.get('access_token'):
            probe = requests.get(f'{FB}/{PAGE_ID}/leadgen_forms', params={'access_token': found['access_token'], 'limit': 1}, timeout=20).json()
            print('leadgen_forms_by_page_token:', json.dumps(probe, ensure_ascii=False))
        probe2 = requests.get(f'{FB}/{PAGE_ID}/leadgen_forms', params={'access_token': token, 'limit': 1}, timeout=20).json()
        print('leadgen_forms_by_user_token:', json.dumps(probe2, ensure_ascii=False))
    except Exception as exc:
        print('error:', repr(exc))
    print()
