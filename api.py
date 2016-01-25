"""HTTP API implementation."""

import csv
import json
import logging
import time
import webapp2

import models


def Multiplex(iterators, selection_func):
  """Iterate over multiple sources and multiplex values.

  Args:
    iterators: list of source iterators
    selection_func: takes a list of values, selects one and returns its index
  Yields:
    Each value from the source iterators, in the order determined by
    selection_func.
  """
  iterators = list(iterators)

  for i, obj in reversed(list(enumerate(iterators))):
    try:
      iterators[i] = [obj, obj.next()]
    except StopIteration:
      del iterators[i]

  while iterators:
    i = selection_func([x[1] for x in iterators])
    yield iterators[i][1]
    try:
      iterators[i][1] = iterators[i][0].next()
    except StopIteration:
      del iterators[i]


class Get(webapp2.RequestHandler):
  """Fetch values from one or more timeseries.

  HTTP parameters:
    expr=<string>
    format={csv,json}
    resolution={full,minute,hour,day}
    start=<unix_timestamp>
    end=<unix_timestamp>
  """

  def get(self):
    output_format = self.request.get('format', 'json')
    assert output_format in ('csv', 'json')

    resolution = self.request.get('resolution', 'full')
    resolution = models.Values.RESOLUTION_NAMES[resolution]

    start = self.request.get('start', None)
    if start:
      start = int(start)
      if start < 0:
        start = int(time.time()) + start
    end = self.request.get('end', None)
    if end:
      end = int(end)
      if end < 0:
        end = int(time.time()) + end

    expr = self.request.get('expr')
    data = models.TimeSeries.FromExpr(expr)

    if output_format == 'csv':
      self.response.content_type = 'text/csv'
      fh = csv.writer(self.response.out)
      # CSV requires us to pre-determine column names
      try:
        group = data.iterkeys().next()
      except StopIteration:
        return
      group_columns = [x.tag_key for x in group]
      fh.writerow(['timestamp'] + group_columns + ['value'])
    elif output_format == 'json':
      self.response.content_type = 'application/json'
      ret = []

    def SelectMinTime(values):
      return min(range(len(values)), key=lambda x: values[x][0])

    streams = {}

    # Get all datastore queries running in parallel first
    for groupings, timeseries in data.iteritems():
      streams[groupings] = [
          x.GetValues(start=start, end=end, resolution=resolution)
          for x in timeseries
      ]

    for groupings in sorted(data.keys()):
      group_values = dict((x.tag_key, x.tag_value) for x in sorted(groupings))

      values = Multiplex(streams[groupings], SelectMinTime)

      if output_format == 'csv':
        group_constants = [group_values[x] for x in group_columns]
        for timestamp, value in values:
          fh.writerow([timestamp] + group_constants + [value])
      elif output_format == 'json':
        ret.append({
            'tags': group_values,
            'timestamps_values': list(values),
        })

    if output_format == 'json':
      json.dump(ret, self.response.out, separators=(',', ':'))


class Put(webapp2.RequestHandler):
  """Add a value to a timeseries.

  HTTP parameters:
    tag=<key>=<value> (repeated)
    value=<integer>
  """

  def _HandleBlock(self, block):
    if 'timestamps_values' not in block:
      now = int(time.time())
      block['timestamps_values'] = [[now, x] for x in block['values']]

    if 'client_timestamp' in block:
      server_timestamp = int(time.time())
      offset = server_timestamp - block['client_timestamp']
      for pair in block['timestamps_values']:
        pair[0] += offset

    tags = [models.Tag.FromStr(x, create=True)
            for x in block['tags']]
    timeseries = models.TimeSeries.GetOrCreate(tags)

    timeseries.AddValues(block['timestamps_values'],
                         offset=block.get('offset', False))

  def post(self):
    content_type = self.request.headers['Content-Type'].split(';', 1)[0]

    if content_type == 'application/x-www-form-urlencoded':
      self._HandleBlock({
          'offset': bool(self.request.get('offset', 0)),
          'tags': self.request.get_all('tag'),
          'values': [int(x) for x in self.request.get_all('value')],
      })

    elif content_type == 'application/json':
      for block in json.loads(self.request.body):
        self._HandleBlock(block)

    else:
      assert False, content_type


app = webapp2.WSGIApplication([
    ('/api/get', Get),
    ('/api/put', Put),
])
