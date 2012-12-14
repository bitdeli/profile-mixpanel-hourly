from bitdeli import profile_events
from bitdeli.protocol import params
from bitdeli.chunkedlist import ChunkedList
from collections import Counter
from itertools import groupby, islice
from operator import itemgetter
import json

PARAMS = params()
PROPERTIES_RETENTION = PARAMS['plan']['retention-days'] * 24
PROFILE_RETENTION = PARAMS['plan']['retention-days']
DROP_PROPERTIES_INTERVAL = 5
MAX_VALUES_PER_PROPERTY = 10

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
            items = [(iter(counts).next(), value, counts)
                     for value, counts in values.iteritems()]
            pivot = max(0, len(values) - MAX_VALUES_PER_PROPERTY)
            if pivot > 0:
                # drop the oldest values, if the number of values exceeds
                # MAX_VALUES_PER_PROPERTY
                items.sort()
                for time, value, counts in items[:pivot]:
                    del values[value]
            # drop timestamps per PROPERTIES_RETENTION
            for time, value, counts in items[pivot:]:
                counts.drop_chunks(lambda x: now - x[0] <= PROPERTIES_RETENTION)
                if not counts:
                    del values[value]
            if not values:
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
            prop_counter.update((unicode(k), unicode(v))\
                                for k, v in event_properties.iteritems())
        for event_type, count in event_counter.iteritems():
            push(events, event_type, hour, count)
        for (prop_key, prop_value), count in prop_counter.iteritems():
            push(properties.setdefault(prop_key, {}), prop_value, hour, count)
    drop_old_properties(hour, profile)
    profile.set_expire(PROFILE_RETENTION)
