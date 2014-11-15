module.exports = function Topic (exchange, topicKey) {
    this.exchange = exchange;
    this.key      = topicKey;
};

Topic.prototype.publish = function (payload) {
    return this.exchange.publish(this.key, payload);
};
