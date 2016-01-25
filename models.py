"""Datastore model definitions and utility functions."""

import bisect
import collections
import itertools
import logging
import re
import StringIO
import sys
import time

from google.appengine.api import memcache
from google.appengine.ext import db


class Error(Exception):
  pass


class InvalidExpression(Error):
  pass


class InvalidSerializedData(Error):
  pass


class Tag(db.Model):
  """A single unique key/value tag pair.

  Immutable once written, so positively cacheable.
  """
  # key_name = string representation of tag key and value

  tag_key = db.StringProperty(required=True)
  tag_value = db.StringProperty(required=True)

  _cache = {}

  @classmethod
  def Get(cls, key, value):
    """Fetch a single Tag.

    May fetch from cache or datastore. Positive but not negative caching.

    Args:
      key: string
      value: string
    Returns:
      Tag object, or None.
    """
    cache_key = '%s=%s' % (key, value)
    obj = cls._cache.get(cache_key, None)
    if obj:
      return obj
    obj = cls.get_by_key_name(cache_key)
    if obj:
      cls._cache[cache_key] = obj
      return obj
    return None

  @classmethod
  def GetOrCreate(cls, key, value):
    """Fetch or create a single Tag.

    May fetch from cache or datastore. Positive but not negative caching.

    Args:
      key: string
      value: string
    Returns:
      Tag object.
    """
    cache_key = '%s=%s' % (key, value)
    obj = cls._cache.get(cache_key, None)
    if obj:
      return obj
    obj = cls.get_or_insert(cache_key, tag_key=key, tag_value=value)
    cls._cache[cache_key] = obj
    return obj

  @classmethod
  def FromStr(cls, tagstring, create=False):
    """Fetch a Tag using a key=value string representation.

    Args:
      tagstring: <key>=<value>
      create: if True, create the Tag if it doesn't exist. Only use from POST
        requests.
    Returns:
      Tag object, or None (if create=False).
    """
    key, value = tagstring.split('=', 1)
    if create:
      return cls.GetOrCreate(key, value)
    else:
      return cls.Get(key, value)

  @classmethod
  def _CacheAndYield(cls, query):
    """Fetch and yield tags from a query, caching the results sa we go.

    Args:
      query: already-running datastore query that returns Tag objects
    Yields:
      Tag objects
    """
    for tag in query:
      cls._cache[tag.key().name()] = tag
      yield tag

  @classmethod
  def FromKey(cls, keystring):
    """Fetch all tags with a given key.

    Args:
      keystring: string
    Returns:
      Iterator of Tag objects.
    """
    query = (cls.all()
             .filter('tag_key =', keystring)
             .run())
    return cls._CacheAndYield(query)

  def __cmp__(self, other):
    return cmp((self.tag_key, self.tag_value),
               (other.tag_key, other.tag_value))

  def __str__(self):
    return '%s=%s' % (self.tag_key, self.tag_value)

  def __repr__(self):
    return str(self)


class TimeSeries(db.Model):
  """A unique set of tags.

  Immutable once written, so positively cacheable.
  """
  _EXPR_RE = re.compile('^\\{(?P<selectors>.*)\\}(\\[(?P<groupings>.*)\\])?$')

  # key_name = string representation of sorted tags
  tags = db.ListProperty(item_type=db.Key, required=True)

  _cache = {}

  @classmethod
  def GetOrCreate(cls, tags):
    """Fetch a single TimeSeries.

    May fetch from cache or datastore. Positive but not negative caching.

    Args:
      tags: list of Tag objects
    Returns:
      TimeSeries object.
    """
    tags.sort(key=lambda x: (x.tag_key, x.tag_value))
    cache_key = ','.join(str(x) for x in tags)
    obj = cls._cache.get(cache_key, None)
    if obj:
      return obj
    obj = cls.get_or_insert(cache_key, tags=[x.key() for x in tags])
    cls._cache[cache_key] = obj
    return obj

  @classmethod
  def FromExpr(cls, expr):
    """Fetch TimeSeries using a string expression.

    Expression format is:
      {<key1>=<value1>,<key2>=<value2>,...}[<key3>,<key4>,...]

    Args:
      expr: string, see format above
    Returns:
      dict of (grouping key,grouping key,...): list of TimeSeries objects
      Empty tuple key if there are no grouping keys
    Raises:
      InvalidExpression: expr not parseable
    """
    parsed = cls._EXPR_RE.match(expr)
    if not parsed:
      raise InvalidExpression

    tags = []
    for key_value in parsed.group('selectors').split(','):
      tags.append(Tag.FromStr(key_value))

    groupings = []
    groupings_str = parsed.group('groupings')
    if groupings_str:
      for grouping in groupings_str.split(','):
        groupings.append(Tag.FromKey(grouping))

    return cls.GetPartial(tags, groupings)

  @classmethod
  def GetPartial(cls, tags, groupings):
    """Get a list of TimeSeries matches for the given tags.

    Does partial matching, e.g. key1=value1 matches the TimeSeries
    key1=value1,key2=value2. This enables forwards-compatibility, i.e. it
    allows data sources to add new tags without breaking existing readers.

    Args:
      tags: list of Tag objects, for selection
      groupings: list of list of Tag objects, for grouping
    Returns:
      dict of (grouping key,grouping key,...): list of TimeSeries objects
      Empty tuple key if there are no grouping keys
    """
    # Build all our query objects, then go through and use them, trying to
    # keep a minimum set in memory at all times.

    # Build and start queries for selectors.
    tag_queries = []
    for tag in tags:
      tag_queries.append(cls.all(keys_only=True)
                         .filter('tags =', tag)
                         .run())

    # Build and start queries for grouping. Also build a dict of sets used later
    # to structure the response.
    tags_by_tag_keys = collections.defaultdict(set)
    grouping_queries = []
    for grouping in groupings:
      or_queries = []
      for tag in grouping:
        tags_by_tag_keys[tag.tag_key].add(tag.key())
        or_queries.append(cls.all(keys_only=True)
                          .filter('tags =', tag)
                          .run())
      grouping_queries.append(or_queries)

    # Pull the results from the selection queries and combine them (AND).
    keys = None
    for query in tag_queries:
      timeseries = set(query)
      if keys is None:
        keys = timeseries
      else:
        keys &= timeseries

    # Pull the results from the grouping queries and combine them (OR within
    # the tags for a group, AND between groups). We eliminate any TimeSeries
    # that lack any values for the grouping tags.
    for or_queries in grouping_queries:
      or_set = set()
      for query in or_queries:
        timeseries = set(query)
        # Pre-filter to trim down things to keep.
        timeseries &= keys
        or_set |= timeseries
      keys &= or_set

    # Fetch all the relevant TimeSeries objects, either from cache or in from
    # datastore in batch. Write anything fetched from datastore back to cache.
    timeseries = []
    to_fetch = []
    for key in keys:
      obj = cls._cache.get(key.name(), None)
      if obj:
        timeseries.append(obj)
      else:
        to_fetch.append(key)
    if to_fetch:
      for obj in cls.get(to_fetch):
        cls._cache[obj.key().name()] = obj
        timeseries.append(obj)

    # Shortcut for queries without grouping.
    if not groupings:
      return {
          (): timeseries,
      }

    # Bucket the TimeSeries objects by their grouping values.
    ret = collections.defaultdict(list)
    for ts in timeseries:
      ts_tags = set(ts.tags)
      intersects = [
          ts_tags & tags
          for tags in tags_by_tag_keys.itervalues()
      ]
      # We handle cases where a given TimeSeries has multiple values for the
      # same tag key. Those go into multiple buckets, hence product().
      for keysets in itertools.product(*intersects):
        tags = tuple(Tag.FromStr(x.name()) for x in keysets)
        ret[tags].append(ts)

    return ret

  @classmethod
  def KeyName(cls, obj):
    if isinstance(obj, db.Model):
      return obj.key().name()
    elif isinstance(obj, db.Key):
      return obj.name()
    else:
      assert False, obj

  def AddValue(self, value, timestamp=None, offset=False):
    """Add a value to this TimeSeries.

    Finds or creates the appropriate Values child object and adds the new
    value to it.

    Args:
      value: integer
      timestamp: UNIX timestamp; defaults to now
      offset: if True, values are offsets from previous value
    """
    timestamp = timestamp or int(time.time())
    Values.AddValue(self, timestamp, value, offset=offset)

  def AddValues(self, timestamp_value_pairs, offset=False):
    """Add values to this TimeSeries.

    Args:
      timestamp_value_pairs: list of (unix_timestamp, value) tuples
      offset: if True, values are offsets from previous value
    """
    if not timestamp_value_pairs:
      return
    Values.AddValues(self, timestamp_value_pairs, offset=offset)

  def _FilterValues(self, values_list, start, end):
    """Filter values based on criteria.

    Separated to move the yield call out of GetValues().

    See GetValues() for args.
    """
    for values in values_list:
      for timestamp, value in values.GetValues():
        if timestamp < start or timestamp > end:
          continue
        yield (timestamp, value)

  def GetValues(self, start=None, end=None, resolution=None):
    """Fetch values from this TimeSeries.

    Args:
      start: UNIX timestamp; defaults to 0
      end: UNIX timestamp; defaults to sys.maxint
      resolution: one of Values.RESOLUTIONS
    Yields:
      (unix_timestamp, value) pairs, in ascending chronological order
    """
    start = start or 0
    end = end or sys.maxint
    resolution = resolution or Values.FULL

    memiter = []
    if resolution in Values.LEADING_BLOCK_IN_MEMCACHE:
      client = memcache.Client()
      namespace = 'TimeSeries:%d' % resolution
      values = client.get(self.KeyName(self), namespace=namespace)
      if values:
        if values.start_time <= start:
          return self._FilterValues([values], start, end)
        memiter = [values]

    query1 = (Values.all()
              .ancestor(self)
              .filter('resolution =', resolution)
              .filter('start_time <=', start)
              .order('-start_time'))
    values1 = query1.run(limit=1)

    query2 = (Values.all()
              .ancestor(self)
              .filter('resolution =', resolution)
              .filter('start_time >', start)
              .filter('start_time <=', end)
              .order('start_time'))
    values2 = query2.run()

    return self._FilterValues(itertools.chain(values1, values2, memiter),
                              start, end)

  def __str__(self):
    return self.key().name()


class Values(db.Model):
  """A chunk of values for a timeseries.

  Subject to read/modify/write which must be transactional.
  """
  # parent = TimeSeries

  FULL = 0
  MINUTE = 1
  HOUR = 2
  DAY = 3

  SECONDS = {
      MINUTE: 60,
      HOUR: 60 * 60,
      DAY: 60 * 60 * 24,
  }

  RESOLUTION_NAMES = {
      'full': FULL,
      'minute': MINUTE,
      'hour': HOUR,
      'day': DAY,
  }

  RESOLUTIONS = {FULL, MINUTE, HOUR, DAY}
  DOWNSAMPLES = {MINUTE, HOUR, DAY}

  # Dict of resolution -> (max seconds in memcache, min seconds in memcache)
  LEADING_BLOCK_IN_MEMCACHE = {
      FULL: (60 * 60 * 24, 20 * 60),
      MINUTE: (60 * 60 * 24, 2 * 60 * 60),
  }

  resolution = db.IntegerProperty(required=True, choices=RESOLUTIONS)

  start_time = db.IntegerProperty(required=True)
  start_value = db.IntegerProperty(required=True)

  end_time = db.IntegerProperty(required=True)
  end_value = db.IntegerProperty(required=True)

  # Encoding: pairs of zigzag base128 varint encodings of deltas from the
  # previous time (in seconds) and value (units unspecified)
  times_and_values = db.BlobProperty()

  _BLOB_LIMIT = 2**16

  @classmethod
  def AddValue(cls, timeseries, timestamp, value, offset=False,
               resolution=FULL):
    """Single value wrapper for AddValues()"""
    return cls.AddValues(timeseries, [[timestamp, value]], offset, resolution)

  @classmethod
  def AddValues(cls, timeseries, timestamp_value_pairs, offset=False,
                resolution=FULL):
    """Find or create a block, then add values.

    Args:
      timeseries: parent TimeSeries object
      timestamp_value_pairs: list of (unix_timestamp, value) tuples
      offset: if True, values are offsets from previous value
      resolution: one of Values.RESOLUTIONS
    """
    if not timestamp_value_pairs:
      return
    if resolution in cls.LEADING_BLOCK_IN_MEMCACHE:
      cls._AddValuesMemcache(timeseries, timestamp_value_pairs, offset,
                             resolution)
    else:
      cls._AddValuesDatastore(timeseries, timestamp_value_pairs, offset,
                              resolution)

  @classmethod
  def _AddValuesMemcache(cls, timeseries, timestamp_value_pairs, offset,
                         resolution):
    """AddValues helper for memcache-backed blocks.

    See AddValues() for arguments.
    """
    client = memcache.Client()
    key = TimeSeries.KeyName(timeseries)
    namespace = 'TimeSeries:%d' % resolution
    values = client.gets(key, namespace=namespace)
    if values:
      values._AddValues(timestamp_value_pairs, offset=offset)
      assert client.cas(key, values, namespace=namespace)
    else:
      values = cls._Create(timeseries, timestamp_value_pairs, resolution)
      client.set(key, values, namespace=namespace)

    max_age, min_age = cls.LEADING_BLOCK_IN_MEMCACHE[resolution]
    now = int(time.time())
    age = now - values.start_time
    if age > max_age:
      new_start = now - min_age
      logging.info('Memcache block is too old (%d > %d); splitting at %d',
                   age, max_age, new_start)
      values._Split(split_timestamp=new_start)
      client.set(key, values, namespace=namespace)

  @classmethod
  @db.transactional()
  def _AddValuesDatastore(cls, timeseries, timestamp_value_pairs, offset,
                          resolution, add_downsamples=True):
    """AddValues helper for datastore-backed blocks.

    See AddValues() for arguments.
    """
    values = (cls.all()
              .ancestor(timeseries)
              .filter('resolution =', resolution)
              .order('-start_time')).fetch(1)
    if values:
      values = values[0]
      values._AddValues(timestamp_value_pairs, offset=offset,
                        add_downsamples=add_downsamples)
    else:
      values = cls._Create(timeseries, timestamp_value_pairs,
                           resolution, add_downsamples=add_downsamples)

    values.save()

  @classmethod
  def _Create(cls, timeseries, timestamp_value_pairs, resolution,
              add_downsamples=True):
    """Factory for new Values object."""
    first_timestamp, first_value = timestamp_value_pairs[0]
    values = Values(
        parent=timeseries,
        resolution=resolution,
        start_time=first_timestamp,
        start_value=first_value,
        end_time=first_timestamp,
        end_value=first_value)

    if add_downsamples and resolution == cls.FULL:
      # Add to downsamples just in case.
      for downsample in cls.DOWNSAMPLES:
        cls.AddValues(timeseries, [[first_timestamp, first_value]],
                      resolution=downsample)

    remaining = timestamp_value_pairs[1:]
    values._AddValues(remaining, add_downsamples=add_downsamples)

    return values

  @classmethod
  def ToZigZag(cls, value):
    """Converts a value to zig zag encoding.

    Args:
      value: positive or negative integer
    Returns:
      Positive integer encoding
    """
    if value < 0:
      return (abs(value) << 1) - 1
    else:
      return value << 1

  @classmethod
  def FromZigZag(cls, value):
    """Converts a value from zig zag encoding.

    Args:
      value: positive encoded integer
    Returns:
      Positive or negative decoded integer
    """
    if value & 0x01:
      return 0 - ((value + 1) >> 1)
    else:
      return value >> 1

  @classmethod
  def ToVarint(cls, value):
    """Encodes an integer as a zig zag, variable-length string.

    Args:
      value: positive or negative integer
    Returns:
      A string with one or more encoded bytes
    """
    value = cls.ToZigZag(value)
    ret = []
    while value >= 2**7:
      ret.append(chr(0x80 | (value % 2**7)))
      value >>= 7
    ret.append(chr(value))
    return ''.join(ret)

  @classmethod
  def FromVarint(cls, fh):
    """Decodes a variable-length, zig zag integer from a file handle.

    Moves the file pointer forward to the byte after the encoded integer.

    Args:
      fh: file handle to read from; use StringIO to use a string
    Returns:
      Positive or negative decoded integer, or None if the stream stops
        immediately
    Raises:
      InvalidSerializedData: stream stops in the middle of a varint
    """
    val = 0
    shift = 0
    while True:
      byte = fh.read(1)
      if not byte:
        if val == 0:
          return None
        else:
          raise InvalidSerializedData('incomplete varint')
      byte_val = ord(byte)
      val |= ((byte_val & 0x7f) << shift)
      if not byte_val & 0x80:
        break
      shift += 7
    return cls.FromZigZag(val)

  def _AddValues(self, timestamp_value_pairs, offset=False,
                 add_downsamples=True):
    """Add values to this block.

    Args:
      timestamp_value_pairs: list of (unix_timestamp, value) tuples
      offset: if True, values are offsets from previous value
      add_downsamples: if True, add downsample values if necessary
    """
    if not timestamp_value_pairs:
      return

    timestamp_value_pairs.sort()

    if offset:
      # Convert to absolute values
      prev_value = self.end_value
      for pair in timestamp_value_pairs:
        pair[1] += prev_value
        prev_value = pair[1]

    if add_downsamples and self.resolution == self.FULL:
      self._CheckAddDownsamples(timestamp_value_pairs)

    parts = [
        self.times_and_values or '',
    ]

    for timestamp, value in timestamp_value_pairs:
      if timestamp < self.start_time:
        logging.warn('Skipping old value (%s): %d < %d',
                     self.key().name(), timestamp, self.start_time)
      parts.append(self.ToVarint(timestamp - self.end_time))
      parts.append(self.ToVarint(value - self.end_value))
      self.end_time = timestamp
      self.end_value = value
    self.times_and_values = ''.join(parts)

    if len(self.times_and_values) > self._BLOB_LIMIT:
      self._Split()

  def _CheckAddDownsamples(self, timestamp_value_pairs):
    """Add downsamples if necessary.

    Args:
      timestamp_value_pairs: sorted list of (unix_timestamp, value) tuples
    """
    # We add a skew factor so we don't have every timeseries trying to add
    # downsamples at the same time.
    skew = hash(self.parent_key().name())

    assert self.resolution == self.FULL
    for resolution, seconds in self.SECONDS.iteritems():
      prev_timestamp = self.end_time
      for timestamp, value in timestamp_value_pairs:
        if (timestamp + skew) / seconds != (prev_timestamp + skew) / seconds:
          # We've crossed a downsample boundary
          Values.AddValue(self.parent_key(), timestamp, value,
                          resolution=resolution)
        prev_timestamp = timestamp

  def GetValues(self):
    """Fetch all values from this block.

    Returns:
      Sorted list of (unix_timestamp, value) tuples.
    Raises:
      InvalidSerializedData: serialized values stops in the middle of a
        timestamp and value pair
    """
    values = [(self.start_time, self.start_value)]
    if not self.times_and_values:
      return values

    last_timestamp = self.start_time
    last_value = self.start_value

    fh = StringIO.StringIO(self.times_and_values)
    while True:
      timestamp = self.FromVarint(fh)
      if timestamp is None:
        break
      value = self.FromVarint(fh)
      if value is None:
        raise InvalidSerializedData('time without value')
      last_timestamp += timestamp
      last_value += value
      values.append((last_timestamp, last_value))

    values.sort(key=lambda x: x[0])

    if self.resolution != self.FULL:
      seconds = self.SECONDS[self.resolution]
      # Clean up the values list for potential duplicates
      i = 1
      while i < len(values):
        if values[i][0] / seconds == values[i - 1][0] / seconds:
          # Latter value is a dupe
          del values[i]
        else:
          i += 1

    return values

  def _Split(self, split_timestamp=None):
    """Split this block into two.

    Decodes the current block and puts the latter half of the values into a
    new block.

    Args:
      split_timestamp: UNIX timestamp around which to split (inclusive in the
        latter block)
    """
    values = self.GetValues()

    if split_timestamp is None:
      split_point = len(values) / 2
    else:
      split_point = bisect.bisect_left(values, (split_timestamp, 0))

    logging.info(
        'Splitting block from TimeSeries=%s, resolution=%d, '
        'split_timestamp=%s, split_point=%d',
        TimeSeries.KeyName(self.parent_key()), self.resolution,
        split_timestamp, split_point)

    old_values = values[:split_point]
    if split_timestamp is None:
      old_obj = self._Create(self.parent_key(),
                             old_values,
                             resolution=self.resolution,
                             add_downsamples=False)
      old_obj.save()
    else:
      # split_timestamp implies that we're doing an uneven block split. To
      # avoid unnecessary fragmentation, add to the leading block in
      # datastore, rather than creating a new one.
      self._AddValuesDatastore(self.parent_key(),
                               old_values,
                               offset=False,
                               resolution=self.resolution,
                               add_downsamples=False)

    self.times_and_values = None
    self.start_time = values[split_point][0]
    self.start_value = values[split_point][1]
    self.end_time = self.start_time
    self.end_value = self.start_value
    self._AddValues(values[split_point + 1:], add_downsamples=False)
