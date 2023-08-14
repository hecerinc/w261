search_space = {
	'maxIter': list(range(5, 6 + 1)),
	'regParam': [.01, .1],
	'elasticNetParam':  [0.0, 0.25, 0.5, 0.75, 1.0]
}
fixed_params = {
	'family': 'binomial', 
	'featuresCol': STANDARD_FEATURE_COLNAME, 
	'labelCol': STANDARD_LABEL_COLNAME
}
best_parameters, best_score = time_series_CV(trainDF_VA, search_space, fixed_params, LogisticRegression, k=3, metric='auc')



- Circular encoding: sine + cosine
- Use display function instead of collecting the data
- see whether the model actually depends on the airport_rank
- Use checkpointing
	- Move the checkpoint to after the casting
- Maybe still use random search, with our own splitting
	- XGBoost
	- lightGBM

- Run the model without any hyperparameter tuning
- Run model interpretability 
- Run error analysis (maybe doesn't support Spark)
	- Find buckets of prediction where prediction perf is low (kinda like heat map MFI)
