import pybgpstream
from datetime import date, datetime, timedelta

class BGP(object):
    
    def __init__( self ):
        """Initialize BGP object with databases URLs"""
        pass
    
    def extractWithdrawalTimeBGPKIT(self, broker, prefix, starttime, hours, verbose):
        t1 = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%S')
        t2 = t1 + timedelta(hours=hours)

        elems = dict()

        try:
            bgp_dumps = broker.query(start_ts=round(t1.timestamp()), end_ts=round(t2.timestamp()), data_type="update")
        except Exception as e:
            return elems

        if verbose:
            print(len(bgp_dumps), t1, t2) 

        for d in bgp_dumps:
            try:
                messages = bgpkit.Parser(url=d.url, 
                                     filters={"prefix": prefix,
                                          "type": "withdraw",
                                          }   
                                      ).parse_all()
            except Exception as e:
                continue

            for m in messages:
                if m.peer_ip not in elems:
                    elems[m.peer_ip] = m.timestamp
                if verbose:
                    print(f"{m.peer_ip},{datetime.fromtimestamp(m.timestamp)}")
  
        return elems

    def extractWithdrawalTimePyBGPStream(self,prefix, starttime, hours, verbose):
        t1 = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%S')
        t2 = t1 + timedelta(hours=hours)

        elems = dict()
        
        if verbose:
            print(prefix, t1, t2)
        
        try:

            stream = pybgpstream.BGPStream(
                from_time=starttime, until_time=t2.isoformat(),
                collectors=["rrc00","rrc14"],
                record_type="updates",
                filter="prefix " + prefix
            )

            #stream.set_data_interface_option("broker", "cache-dir", "cache")

        except Exception as e:
            print(e)
            return elems


        for m in stream:

            if m.type != 'W':
                continue

            peer_ip = m.peer_address

            if peer_ip not in elems:
                elems[peer_ip] = m.time
            if verbose:
                print(f"{peer_ip},{m.time}")

        return elems

    
def main():
    
    #broker = broker = bgpkit.Broker()
    
    bgp = BGP()
    
    elements = bgp.extractWithdrawalTimePyBGPStream('194.133.122.0/24', '2018-01-01T01:09:53', 1, True)
    
    print(elements)
    
if __name__ == "__main__":
    main()

