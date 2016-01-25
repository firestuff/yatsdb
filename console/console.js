var Console = function(container) {
  this.container_ = container;
  this.client_ = new TSDBClient();
  this.loadJSAPI_();
};


Console.prototype.loadJSAPI_ = function() {
  google.load('visualization', '1.1', {
    packages: ['corechart'],
    callback: this.jsapiLoaded_.bind(this),
  });
};


Console.prototype.jsapiLoaded_ = function() {
  var exprContainer = document.createElement('div');
  exprContainer.className = 'expr';
  this.container_.appendChild(exprContainer);

  var form = document.createElement('form');
  exprContainer.appendChild(form);

  this.exprInput_ = document.createElement('input');
  form.appendChild(this.exprInput_);
  form.addEventListener('submit', this.onExprChange_.bind(this));

  var chartContainer = document.createElement('div');
  this.container_.appendChild(chartContainer);

  this.charts_ = [
    {
      'title': 'Last 20 minutes',
      'seconds': 120 * 10,
      'resolution': 'full',
    },
    {
      'title': 'Last 2 hours',
      'seconds': 120 * 60,
      'resolution': 'minute',
    },
    {
      'title': 'Last 5 days',
      'seconds': 120 * 60 * 60,
      'resolution': 'hour',
    },
    {
      'title': 'Last 120 days',
      'seconds': 120 * 60 * 60 * 24,
      'resolution': 'day',
    },
  ];
  for (var i = 0; i < this.charts_.length; i++) {
    var obj = this.charts_[i];
    obj.container = document.createElement('div');
    obj.container.className = 'chart';
    chartContainer.appendChild(obj.container);
  }

  this.expr_ = decodeURIComponent(window.location.hash.substring(1));
  // Ensure that the URL is properly encoded for copy & paste.
  window.location.hash = encodeURIComponent(this.expr_);
  this.exprInput_.value = this.expr_;
  window.addEventListener('hashchange', this.onHashChange_.bind(this));

  this.loadCharts_();
};


Console.prototype.onExprChange_ = function(e) {
  window.location.hash = encodeURIComponent(this.exprInput_.value);
  e.preventDefault();
};


Console.prototype.onHashChange_ = function(e) {
  this.expr_ = decodeURIComponent(window.location.hash.substring(1));
  this.exprInput_.value = this.expr_;
  this.loadCharts_();
};


Console.prototype.loadCharts_ = function() {
  if (!this.expr_) {
    return;
  }

  for (var i = 0; i < this.charts_.length; i++) {
    var options = this.charts_[i];
    if (options.instance) {
      options.instance.destroy();
    }
    options.instance = new Chart(this.client_, this.expr_, options);
  }
};


var Chart = function(client, expr, options) {
  this.options_ = options;
  this.watch_ = client.watch(
      expr, this.options_.seconds, this.options_.resolution,
      this.drawChart_.bind(this));
};


Chart.prototype.drawChart_ = function(data) {
  var dataTable = new google.visualization.DataTable();
  dataTable.addColumn('date', 'Timestamp');

  for (var i = data.length - 1; i >= 0; i--) {
    var ts = data[i];
    if (!ts.timestamps_values.length) {
      data.splice(i, 1);
      continue;
    }
    var tags = [];
    for (var key in ts.tags) {
      tags.push(key + '=' + ts.tags[key]);
    }
    dataTable.addColumn('number', tags.join(','));
  }
  for (var i = 0; i < data.length; i++) {
    var ts = data[i];
    for (var j = 0; j < ts.timestamps_values.length; j++) {
      var row = [new Date(ts.timestamps_values[j][0] * 1000)];
      for (var k = 0; k < i; k++) {
        row.push(null);
      }
      row.push(ts.timestamps_values[j][1]);
      for (var k = i + 1; k < data.length; k++) {
        row.push(null);
      }
      dataTable.addRow(row);
    }
  };

  var options = {
    title: this.options_.title,
    legend: {
      position: 'bottom'
    },
    hAxis: {
      gridlines: {
        count: -1,
      },
    },
    explorer: {
      actions: [
          'dragToZoom',
          'rightClickToReset',
      ],
    },
  };

  // It'd be great to use the material design charts here, but they fail to load
  // ~25% of the time, which is a non-starter.
  if (this.chartObj_) {
    this.chartObj_.clearChart();
  }
  this.chartObj_ = new google.visualization.LineChart(this.options_.container);
  this.chartObj_.draw(dataTable, options);
};


Chart.prototype.destroy = function() {
  this.watch_.destroy();
  if (this.chartObj_) {
    this.chartObj_.clearChart();
  }
};


document.addEventListener('DOMContentLoaded', function(e) {
  new Console(document.getElementById('container'));
});
