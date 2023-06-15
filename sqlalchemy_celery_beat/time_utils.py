# Copied from pytz with some changes

def localize(tzinfo, dt, is_dst=False):
    '''Convert naive time to local time'''
    if dt.tzinfo is not None:
        raise ValueError('Not naive datetime (tzinfo is already set)')
    return dt.replace(tzinfo=tzinfo)


def normalize(tzinfo, dt, is_dst=False):
    '''Correct the timezone information on the given datetime.

    This is normally a no-op, as StaticTzInfo timezones never have
    ambiguous cases to correct:

    >>> from pytz import timezone
    >>> gmt = timezone('GMT')
    >>> isinstance(gmt, StaticTzInfo)
    True
    >>> dt = datetime(2011, 5, 8, 1, 2, 3, tzinfo=gmt)
    >>> gmt.normalize(dt) is dt
    True

    The supported method of converting between timezones is to use
    datetime.astimezone(). Currently normalize() also works:

    >>> la = timezone('America/Los_Angeles')
    >>> dt = la.localize(datetime(2011, 5, 7, 1, 2, 3))
    >>> fmt = '%Y-%m-%d %H:%M:%S %Z (%z)'
    >>> gmt.normalize(dt).strftime(fmt)
    '2011-05-07 08:02:03 GMT (+0000)'
    '''
    if dt.tzinfo is tzinfo:
        return dt
    if dt.tzinfo is None:
        raise ValueError('Naive time - no tzinfo set')
    return dt.astimezone(tzinfo)
