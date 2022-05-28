import appdirs

RPKI_ARCHIVE_URLS = [ 
        'https://ftp.ripe.net/ripe/rpki/afrinic.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        'https://ftp.ripe.net/ripe/rpki/apnic.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        'https://ftp.ripe.net/ripe/rpki/arin.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        'https://ftp.ripe.net/ripe/rpki/lacnic.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        'https://ftp.ripe.net/ripe/rpki/ripencc.tal/{year:04d}/{month:02d}/{day:02d}/roas.csv',
        ]

DEFAULT_RPKI_URLS = [ 
        'https://rpki.gin.ntt.net/api/export.json'
        ]

CACHE_DIR = appdirs.user_cache_dir('rov', 'IHR')
DEFAULT_RPKI_DIR = CACHE_DIR+'/db/rpki/'
RPKI_FNAME = '*.*'
TMP_DIR = 'tmp/'
OUTPUT_DIR = 'data/'
ROA_GET_URL = 'http://45.129.227.23:5000/search?prefix='
RIB_SOURCE_URL = 'http://archive.routeviews.org/route-views.bdix/bgpdata/{0}.{1}/RIBS/rib.{0}{1}{2}.0000.bz2'
OUTPUT_FILE = 'data/{}.csv'