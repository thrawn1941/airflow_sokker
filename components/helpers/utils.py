import datetime
def get_auth():
    result = dict()
    filepath="config/auth"
    with open(filepath, 'r') as file:
        for line in file:
            data = line.strip('\n').split('=')
            if data[1].isdigit():
                data[1]= int(data[1])
            else:
                data[1] = data[1].strip("'")
            result[data[0]] = data[1]
    result['remember'] = False
    return result

def last_thursday():
    dt = datetime.datetime.now()
    days_to_subtract = (dt.weekday() - 3) % 7
    thursday = dt - datetime.timedelta(days=days_to_subtract)
    return thursday.strftime("%Y-%m-%d")

def get_teamid():
    filepath = "config/teamid"
    with open(filepath, 'r') as file:
        teamid = file.read().strip()
    return teamid
