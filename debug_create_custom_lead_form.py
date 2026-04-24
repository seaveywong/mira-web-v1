from api.ad_templates import create_custom_lead_form_for_page

form_id = create_custom_lead_form_for_page(
    '116581311406362',
    'HistoryV085 Debug Form',
    [
        {'type': 'EMAIL', 'key': 'email'},
        {'type': 'PHONE', 'key': 'phone_number'},
        {'type': 'CUSTOM', 'label': 'What product are you interested in?'}
    ],
    token='',
    privacy_url='https://cutt.ly/Stp7Nem0',
    follow_up_url='https://cutt.ly/Stp7Nem0',
    locale='en_US',
)
print('form_id=' + str(form_id))
