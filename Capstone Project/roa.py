import random
import requests
import config

class ROA(object):
    
    def __init__( self ):
        """Initialize ROA object with databases URLs"""
        pass

    def getROACreationDates(self, prefix):
        response = requests.get(config.ROA_GET_URL + prefix)
        d = response.json()
        dates = []

        if len(d['data']) < 1:
            return None
        elif len(d['data']) > 5:
            d['data'] = random.choices(d['data'], k=3)

        for roa in d['data']:
            #notbefore = datetime.fromisoformat(roa['notbefore'])
            #notafter = datetime.fromisoformat(roa['notafter'])
            dates.append(roa['notbefore'])

        return {'prefix':prefix,'tal': roa['tal'], 'dates':dates}
    
def main():
    args = sys.argv[1:]
    
    roa = ROA()
    
if __name__ == "__main__":
    main()

    

