const ONE_HOUR_MS = 60 * 60 * 1000;
const TEN_MINUTES_MS = 10 * 60 * 1000;
const LOCALE = "en-US";
const TIME_ZONE = "America/Vancouver";
const LAUNCH_DAY = Date.parse("2023-12-30T00:00:00.000-08:00");

function getLocalMidnightNDaysAgo(n) {
  const midnight = new Date(Date.now() - n * 24 * 60 * 60 * 1000);
  midnight.setHours(0);
  midnight.setMinutes(0);
  midnight.setSeconds(0);
  midnight.setMilliseconds(0);

  return midnight.getTime();
}

function getInitialTimeLastFetched() {
  const ONE_WEEK_AGO = getLocalMidnightNDaysAgo(7);

  return new Date(Math.max(LAUNCH_DAY, ONE_WEEK_AGO) - ONE_HOUR_MS);
}

module.exports = {
  ONE_HOUR_MS,
  TEN_MINUTES_MS,
  LOCALE,
  TIME_ZONE,
  LAUNCH_DAY,
  getLocalMidnightNDaysAgo,
  getInitialTimeLastFetched,
};
