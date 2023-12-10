const fs = require("fs");
const { performance } = require("perf_hooks");
const express = require("express");
const cors = require("cors");
const { MongoClient, ServerApiVersion } = require("mongodb");
const NodeCache = require("node-cache");
const { keyBy, groupBy, pick, map, uniqBy, sortBy, fromPairs, toPairs, filter, dropRight, forEach } = require("lodash");
const { CACHE_KEYS } = require("./constants/cache-keys.js");
const { log } = require("./constants/log.js");
const { getLocalMidnightNDaysAgo, getInitialTimeLastFetched } = require("./constants/time.js");
const { ONE_HOUR_MS, TEN_MINUTES_MS, LOCALE, TIME_ZONE, LAUNCH_DAY } = require("./constants/time.js");

const { MONGO_USER, MONGO_PASS, MONGO_ADDR } = process.env;
const uri = getUri(MONGO_USER, MONGO_PASS, MONGO_ADDR);
const client = new MongoClient(uri, {
  serverApi: { version: ServerApiVersion.v1, strict: true, deprecationErrors: true },
});

const vehicleCounts = client.db("dev").collection("vehiclecounts");
const cache = new NodeCache();
let timeLastFetched = getInitialTimeLastFetched();
const cameraData = loadCameraData();
const UPDATE_INTERVAL_MS = TEN_MINUTES_MS;

const app = express();
const port = 3001;
app.use(cors());

app.get("/api/health/", async (req, res) => {
  return res.json({ message: "API is up" });
});

app.get("/api/data/", async (req, res) => {
  return res.json({ data: cache.get(CACHE_KEYS.PROCESSED_DATA) ?? {} });
});

app.listen(port, () => {
  log(`Listening on port ${port}`);
});

log(`Updating every ${UPDATE_INTERVAL_MS / 60000} minutes`);
setInterval(fetchProcessAndCacheNewData, UPDATE_INTERVAL_MS);
setTimeout(fetchProcessAndCacheNewData);

async function fetchProcessAndCacheNewData() {
  try {
    const rollingWeek = await fetchAndCacheRecords();
    await processAndCacheData(rollingWeek);
  } catch (e) {
    console.error(e);
  }
}

async function fetchAndCacheRecords() {
  const t0 = performance.now();
  const query = { timestamp: { $gt: timeLastFetched } };
  timeLastFetched = new Date();
  const fetched = await vehicleCounts.find(query).toArray();

  const cached = cache.get(CACHE_KEYS.VEHICLE_COUNTS) ?? [];
  const combined = [...cached, ...fetched];
  const uniqueCombined = uniqBy(combined, ({ _id }) => _id.toHexString());
  const rollingWeek = filterRollingWeek(uniqueCombined);
  cache.set(CACHE_KEYS.VEHICLE_COUNTS, rollingWeek);

  const timeTaken = `(${Math.round(performance.now() - t0) / 1000}s)`;
  log(`Fetched ${fetched.length} records after ${localeString(query.timestamp.$gt)} ${timeTaken}`);

  return rollingWeek;
}

async function processAndCacheData(rollingWeek) {
  const t0 = performance.now();
  const quantized = map(rollingWeek, ({ intersection, timestamp, vehicleCount }) => ({
    intersection,
    timestamp: roundToNearestQuarterHour(timestamp),
    vehicleCount,
  }));
  const groupedByIntersection = groupBy(quantized, "intersection");
  const pairs = toPairs(groupedByIntersection);
  const sortedPairs = sortBy(pairs, ([intersectionName, _]) => intersectionName);
  const processedIntersections = map(sortedPairs, ([intersectionName, vehicleCounts]) => [
    intersectionName,
    { location: getLocation(intersectionName), data: getOrganizedCounts(vehicleCounts) },
  ]);
  const processedData = fromPairs(processedIntersections);
  cache.set(CACHE_KEYS.PROCESSED_DATA, processedData);

  const timeTaken = `(${Math.round(performance.now() - t0) / 1000}s)`;
  log(`Processed ${processedIntersections.length} intersections ${timeTaken}`);
}

function filterRollingWeek(vehicleCounts) {
  const dataCutoffTime = getLocalMidnightNDaysAgo(7) - ONE_HOUR_MS;
  const filteredData = filter(vehicleCounts, ({ timestamp }) => timestamp.getTime() >= dataCutoffTime);

  return sortBy(filteredData, ({ timestamp }) => timestamp.getTime());
}

function getLocation(intersectionName) {
  return cameraData[intersectionName]["location"];
}

function getOrganizedCounts(countsArray) {
  const dataCutoffTime = Math.max(LAUNCH_DAY, getLocalMidnightNDaysAgo(7));
  const filteredData = filter(countsArray, ({ timestamp }) => timestamp.getTime() >= dataCutoffTime);
  const dtos = map(filteredData, (c) => pick(c, ["timestamp", "vehicleCount"]));
  const uniques = uniqBy(dtos, ({ timestamp }) => timestamp.getTime());
  const sortedCounts = sortBy(uniques, ({ timestamp }) => timestamp.getTime());
  const countsByDay = getCountsByDay(sortedCounts);

  return countsByDay;
}

function roundToNearestQuarterHour(date) {
  const msPerQuarterHour = 15 * 60 * 1000;

  return new Date(Math.round(date.getTime() / msPerQuarterHour) * msPerQuarterHour);
}

function getCountsByDay(sortedCounts) {
  const countsByDay = groupBy(sortedCounts, ({ timestamp }) => getYYYYMMDDDate(timestamp));
  const pairs = toPairs(countsByDay);
  const compactCounts = map(pairs, ([date, vehicleCounts]) => {
    return [date, map(vehicleCounts, "vehicleCount")];
  });

  const countsLengths = dropRight(map(compactCounts, ([_, counts]) => counts.length));
  forEach(countsLengths, (countsLength) => {
    if (countsLength !== 96) {
      throw `[ERROR] Length check failed for full-day compact count array: ${JSON.stringify(compactCounts)}`;
    }
  });

  return fromPairs(compactCounts);
}

function getYYYYMMDDDate(timestamp) {
  const YYYY = timestamp.getFullYear();
  const mm = timestamp.getMonth() + 1;
  const dd = timestamp.getDate();

  return `${YYYY}-${mm}-${dd}`;
}

function getUri(user, pass, addr) {
  return `mongodb+srv://${user}:${pass}@${addr}/?retryWrites=true&w=majority`;
}

function loadCameraData() {
  const jsonData = fs.readFileSync("./data/camera_data.json");
  const jsonParsed = JSON.parse(jsonData);

  return keyBy(jsonParsed, "name");
}

function localeString(date) {
  return date.toLocaleString(LOCALE, { timeZone: TIME_ZONE });
}
