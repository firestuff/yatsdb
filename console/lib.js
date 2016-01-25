var TSDBClient = function(opt_baseURL) {
  this.baseURL_ = opt_baseURL || '/api';
};


TSDBClient.prototype.watch = function(
    expr, windowSeconds, resolution, callback) {
  return new TSDBWatch(
      this.baseURL_, expr, windowSeconds, resolution, callback);
};


var TSDBWatch = function(baseURL, expr, windowSeconds, resolution, callback) {
  this.url_ = baseURL + '/get?start=-' + windowSeconds.toString() +
      '&resolution=' + encodeURIComponent(resolution) +
      '&expr=' + encodeURIComponent(expr);
  this.callback_ = callback;
  this.refresh_ = windowSeconds / 40;
  this.delay_ = TSDBWatch.MIN_DELAY;
  this.sendRequest_();
};


TSDBWatch.MIN_DELAY = 0.5;
TSDBWatch.MAX_DELAY = 32.0;
TSDBWatch.DELAY_MULT = 2;


TSDBWatch.prototype.destroy = function() {
  if (this.timer_) {
    window.clearTimeout(this.timer_);
  }
};


TSDBWatch.prototype.sendRequest_ = function() {
  this.timer_ = null;

  var xhr = new XMLHttpRequest();
  xhr.responseType = 'json';
  xhr.timeout = 30 * 1000;
  xhr.open('GET', this.url_);
  xhr.addEventListener('load', this.onLoad_.bind(this));
  xhr.addEventListener('error', this.retry_.bind(this));
  xhr.addEventListener('timeout', this.retry_.bind(this));
  xhr.send();
};


TSDBWatch.prototype.retry_ = function(e) {
  if (this.timer_) {
    console.log('Duplicate retry call');
    return;
  }
  this.timer_ = window.setTimeout(
      this.sendRequest_.bind(this),
      this.delay_ * 1000);
  this.delay_ = Math.min(this.delay_ * TSDBWatch.DELAY_MULT,
                         TSDBWatch.MAX_DELAY);
};


TSDBWatch.prototype.onLoad_ = function(e) {
  var data = e.target.response;

  if (!data) {
    this.retry_();
    return;
  }

  this.delay_ = TSDBWatch.MIN_DELAY;

  this.callback_(data);

  this.timer_ = window.setTimeout(
      this.sendRequest_.bind(this),
      this.refresh_ * 1000);
}
