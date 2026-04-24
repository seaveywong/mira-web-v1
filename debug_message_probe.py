import json
import sqlite3
import requests
from core.database import decrypt_token

DB = '/opt/mira/data/mira.db'
PAGE_ID = '116581311406362'
TOKEN_ID = 18
FB = 'https://graph.facebook.com/v25.0'

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
row = conn.execute("SELECT access_token_enc FROM fb_tokens WHERE id=?", (TOKEN_ID,)).fetchone()
conn.close()
raw = decrypt_token(row['access_token_enc'])
pages = requests.get(f'{FB}/me/accounts', params={'access_token': raw, 'fields': 'id,name,access_token,tasks', 'limit': 200}, timeout=20).json()
page = next((p for p in pages.get('data', []) or [] if str(p.get('id')) == PAGE_ID), None)
print('page_ctx=', json.dumps(page, ensure_ascii=False))
for label, token in [('user', raw), ('page', page.get('access_token') if page else '')]:
    if not token:
        continue
    print('===', label, '===')
    for fields in ['id,name,messaging_feature_status,features', 'id,name,can_post,messenger_ads_default_page_welcome_message', 'id,name']:
        data = requests.get(f'{FB}/{PAGE_ID}', params={'access_token': token, 'fields': fields}, timeout=20).json()
        print(fields, '=>', json.dumps(data, ensure_ascii=False))
