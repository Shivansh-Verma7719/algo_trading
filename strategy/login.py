import json
import pyotp
from shoonya import ShoonyaApiPy


def login():
    creds_shoonya = {}
    with open('creds_shoonya.json', 'r') as f:
        creds_shoonya = json.load(f)  
    if creds_shoonya == {}:
        raise Exception("No creds found")
    totp = pyotp.totp.TOTP(creds_shoonya["totp"]).now()
    client = ShoonyaApiPy()
    client.login(userid=creds_shoonya["user_id"], password=creds_shoonya["password"], twoFA=totp, vendor_code=creds_shoonya["Vendor_code"], api_secret=creds_shoonya["API_KEY"], imei=creds_shoonya["IMEI"])
    return client 