function log(val) {
  console.log(`${prefix()}`, val);
}

function prefix() {
  return `[${new Date().toLocaleTimeString()}]`;
}

module.exports = { log };
