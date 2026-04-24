from api.ad_templates import create_custom_lead_form_for_page

form_id = create_custom_lead_form_for_page(
    '116581311406362',
    'Request More Information',
    [
        {'type': 'FULL_NAME'},
        {'type': 'EMAIL'},
        {'type': 'PHONE'}
    ],
    privacy_url='https://policies.google.com/privacy',
    follow_up_url='https://www.facebook.com/116581311406362',
    locale='en_US',
)
print('form_id=' + str(form_id))
