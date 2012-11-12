from bitdeli import profile_events
from bitdeli.protocol import params
from bitdeli.chunkedlist import ChunkedList
from collections import Counter
from itertools import groupby
from operator import itemgetter
import json

PARAMS = params()
PROPERTIES_RETENTION = PARAMS['plan']['retention-days'] * 24
PROFILE_RETENTION = PARAMS['plan']['retention-days']
DROP_PROPERTIES_INTERVAL = 5

def parse(events):
    for event in events:
        e = json.loads(event.object.data)
        prop = e['properties']
        prop.pop('distinct_id', None)
        hour = prop.pop('time') / 3600
        yield hour, e['event'], prop

def push(d, key, hour, count):
    key = key if isinstance(key, basestring) else str(key)
    if key in d:
        c = d[key]
    else:
        c = d[key] = ChunkedList()
    c.push([(hour, count)])

def drop_old_properties(now, profile):
    profile['_drop_properties'] = p = profile.get('_drop_properties',
                                                  DROP_PROPERTIES_INTERVAL) - 1
    if p == 0:
        profile['_drop_properties'] = DROP_PROPERTIES_INTERVAL
        properties = profile['properties']
        empty_keys = []
        for key, values in properties.iteritems():
            empty_values = []
            prop = properties[key]
            for value, counts in values.iteritems():
                i = 0
                for hour, count in counts:
                    if now - hour <= PROPERTIES_RETENTION:
                        break
                    i += 1
                if 0 < i < len(counts):
                    prop[value] = counts[i:]
                elif i > 0:
                    empty_values.append(value)
            for value in empty_values:
                del prop[value]
            if not prop:
                empty_keys.append(key)
        for key in empty_keys:
            del properties[key]

head = itemgetter(0)
for profile, daily_events in profile_events():
    properties = profile.setdefault('properties', {})
    events = profile.setdefault('events', {})
    hourly = sorted(parse(daily_events), key=head)
    for hour, hourly_events in groupby(hourly, head):
        event_counter = Counter()
        prop_counter = Counter()
        for hour, event_type, event_properties in hourly_events:
            event_counter[event_type] += 1
            prop_counter.update(event_properties.iteritems())
        for event_type, count in event_counter.iteritems():
            push(events, event_type, hour, count)
        for (prop_key, prop_value), count in prop_counter.iteritems():
            push(properties.setdefault(prop_key, {}), prop_value, hour, count)
    drop_old_properties(hour, profile)
    profile.set_expire(PROFILE_RETENTION)
