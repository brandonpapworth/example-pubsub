module.exports = function Queue (exchange, filterFunc) {
    this.exchange   = exchange;
    this.filterFunc = filterFunc;
};

Queue.prototype.fullQuery = function () {
    return this.exchange.fullQuery(this.filterFunc);
};

Queue.prototype.subscribe = function (iterFunc) {
    return this.exchange.subscribe(this.filterFunc, iterFunc);
};
