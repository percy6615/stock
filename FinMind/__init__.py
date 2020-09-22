# __init__

__version__ = '1.0.85'

from Initial import log_setting

logger = log_setting.logger


def api():
    # url = 'http://finmindapi.servebeer.com/api/data'
    url = 'http://api.finmindtrade.com/api/v2/data'
    logger.info('call {}'.format(url))
    return url

