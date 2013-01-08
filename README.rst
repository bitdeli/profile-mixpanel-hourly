
Mixpanel Hourly
---------------

Raw events from `Mixpanel <https://mixpanel.com>`_, aggregated by hour.

- **Source:** `Mixpanel Data Export API <https://mixpanel.com/docs/api-documentation/data-export-api>`_

- **Historical data:** All historical data.

- **Retention:**
    - *Premium*: 365 days of properties and inactive users.
    - *Advanced*: 90 days of properties and inactive users.
    - *Basic*: 30 days of properties and inactive users.

  Inactive users who have not produced any events for the last *N* days, where *N*
  depends on the plan as defined above, are purged from profiles.

  Correspondingly, *hourly-stats* for properties, as defined below, contain
  statistics for the last *N* days depending on the plan.

  Note that all plans include unlimited *hourly-stats* for events.

- **Schema:**
    .. code-block:: python

         {
             "properties": {
                 "property-type": {
                     "property-value": hourly-stats
                     ...
                 }
                 ...
             },
             "events": {
                "event-type": hourly-stats
                ...
             }
         }

    where *hourly-stats* is a :mod:`bitdeli.lazylist` containing `(hour, count)` pairs.
    Note that only the ten newest *property-values* are stored for each *property-type*.
    For instance, the last 10 countries where the user has used the application.

- **Update interval:** 24 hours
  (limited by the `Mixpanel Data Export API <https://mixpanel.com/docs/api-documentation/data-export-api>`_)

- **Code:** `bitdeli/profile-mixpanel-hourly <https://github.com/bitdeli/profile-mixpanel-hourly>`_


.. image:: https://d2weczhvl823v0.cloudfront.net/bitdeli/profile-mixpanel-hourly/trend.png
   :alt: Bitdeli badge
   :target: https://bitdeli.com/free

