#!/usr/bin/python2.7

import argparse
import glob
import socket
import sys
import time

sys.path.append('..')

from lib import tsdblib


parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True)
parser.add_argument('--base_url', required=True)
FLAGS = parser.parse_args()


class Collector(object):

  def __init__(self):
    self._start_time = int(time.time())
    self._base_tags = self._BaseTags()
    self._client = tsdblib.TSDBClient(FLAGS.base_url,
                                      jit_callback=self._PutValues)

  def _GetHostname(self):
    return socket.gethostname()

  def _GetMACAddresses(self):
    for path in glob.iglob('/sys/class/net/*/address'):
      mac_address = open(path, 'r').read().strip()
      if mac_address == '00:00:00:00:00:00':
        continue
      yield mac_address

  def _GetCollectorUptime(self):
    return int(time.time()) - self._start_time

  def _GetSystemUptime(self):
    return int(open('/proc/uptime', 'r').read().split('.', 1)[0])

  def _GetLoadAverage1m(self):
    return int(float(open('/proc/loadavg', 'r').read().split(' ', 1)[0]) * 100)

  def _BaseTags(self):
    return [
      ('project', FLAGS.project),
      ('hostname', self._GetHostname()),
    ] + [
      ('mac_address', mac_address)
      for mac_address in self._GetMACAddresses()
    ]

  def _CycleValues(self):
    return [
      ('collector_uptime_seconds', self._GetCollectorUptime()),
      ('system_uptime_seconds', self._GetSystemUptime()),
      ('system_load_average_1m', self._GetLoadAverage1m()),
    ]

  def _PutValues(self):
    cycle_values = self._CycleValues()
    for name, value in cycle_values:
      self._client.PutValue(self._base_tags + [('value', name)], value)

  def Loop(self):
    time.sleep(9999999999)


Collector().Loop()
