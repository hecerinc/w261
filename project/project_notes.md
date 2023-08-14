- Add a `IS_HOLIDAY` column
- Transform into `HOLIDAY_RUSH`
- Adds a `TIME_SLOT` column to identify:
  - morning
  - mid-morning
  - afternoon
  - evening
  - night
- Adds a `SEASON` column based on month (`winter`, `spring`, `summer`, `fall`)
- Adds the folowing columns
  - `delta_depart_2h`, 
  - `delta_depart_4h`, 
  - `delta_depart`, 
  - `weather_condition_measured_time_from_depart`: used to calculate the next one:
  - *I think this might be making a wrong assumption, namely because it's calculating based on the previous `delta_depart`, but we don't really know what the `DATE` column is
  - `closest_depart_time_weather_measurement`:
  - `weather_condition_measured_time_from_depart`: int
- RFM analysis
  - Based on a hypothesis that the recency, frequency, and delay time are predictors for future delays (**Is this hypothesis right?**)

# TODOs

- [x] Add Hyperopt
- [x] Logistic regression
- [x] Balanced accuracy
- [x] Correct CrossValidation for time series
- [x] Check other notebooks

