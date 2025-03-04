import re
def is_valid_email(email):
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
status = True if is_valid_email('ckchopasxyx.wx.in') else "Invalid"
if(status!= "Invalid"):
        print(1)